// Package libvirtsrc talks to libvirt on the TrueNAS host and turns what it
// finds into model.Snapshot values.
//
// Everything in here is read-only. There are no calls that create, destroy,
// define, undefine or otherwise change a domain, and there must never be —
// the container holds a read-write libvirt handle purely because guest agent
// commands are classed as writes, not because it needs to change anything.
package libvirtsrc

import (
	"encoding/xml"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/digitalocean/go-libvirt"
	"github.com/digitalocean/go-libvirt/socket/dialers"

	"vmpeek/internal/agent"
	"vmpeek/internal/model"
)

// Memory stat tags from libvirt-domain.h. We spell them out rather than
// leaning on the generated constants so the meaning is visible at the point
// of use — these numbers are stable libvirt ABI.
const (
	memStatUnused    int32 = 4 // MemFree in the guest
	memStatAvailable int32 = 5 // MemTotal in the guest
	memStatUsable    int32 = 8 // MemAvailable — free plus reclaimable cache
)

// guestAgentChannel is the virtio-serial channel TrueNAS adds to every VM.
const guestAgentChannel = "org.qemu.guest_agent.0"

// domainNameRE splits TrueNAS's "<vm_id>_<vm_name>" domain naming.
var domainNameRE = regexp.MustCompile(`^(\d+)_(.+)$`)

// Config is everything the Source needs to know.
type Config struct {
	// Socket is the libvirt unix socket. On TrueNAS this is the non-standard
	// /run/truenas_libvirt/libvirt-sock, not /var/run/libvirt/libvirt-sock.
	Socket string

	// AgentTimeout is how many seconds to give a guest agent command.
	// libvirt treats -2 as block forever and -1 as its own default; we always
	// want a real number here so one wedged guest can't stall a poll.
	AgentTimeout int32

	// Concurrency caps how many VMs we interrogate at once.
	Concurrency int

	Log *slog.Logger
}

// Source is a live connection to libvirt, plus the small amount of state we
// need to work out rates of change between polls.
type Source struct {
	cfg Config
	log *slog.Logger

	mu   sync.Mutex
	conn *libvirt.Libvirt

	// prevCPU holds the last CPU time sample per domain, so we can turn
	// libvirt's cumulative nanosecond counter into a percentage.
	prevCPU map[string]cpuSample

	// lastAgent remembers each VM's agent state so we can log the transition
	// once instead of moaning about the same missing agent every 30 seconds.
	lastAgent map[string]model.AgentState
}

type cpuSample struct {
	cpuTime uint64
	at      time.Time
}

// New builds a Source. It does not connect — Poll does that on demand and
// reconnects if the socket goes away.
func New(cfg Config) *Source {
	if cfg.AgentTimeout <= 0 {
		cfg.AgentTimeout = 5
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 8
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Source{
		cfg:       cfg,
		log:       cfg.Log,
		prevCPU:   map[string]cpuSample{},
		lastAgent: map[string]model.AgentState{},
	}
}

// Close drops the libvirt connection.
func (s *Source) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.Disconnect()
		s.conn = nil
	}
}

// connect returns a live libvirt handle, dialling if needed.
//
// /run is tmpfs, so the socket directory is rebuilt at boot and libvirtd
// recreates the socket when it restarts. Reconnecting rather than assuming
// the handle stays good is what lets the container survive a NAS reboot
// without needing a restart itself.
func (s *Source) connect() (*libvirt.Libvirt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn != nil && s.conn.IsConnected() {
		return s.conn, nil
	}
	if s.conn != nil {
		_ = s.conn.Disconnect()
		s.conn = nil
	}

	l := libvirt.NewWithDialer(dialers.NewLocal(
		dialers.WithSocket(s.cfg.Socket),
		dialers.WithLocalTimeout(10*time.Second),
	))
	if err := l.Connect(); err != nil {
		return nil, fmt.Errorf("connect to %s: %w", s.cfg.Socket, err)
	}
	s.log.Info("connected to libvirt", "socket", s.cfg.Socket)
	s.conn = l
	return l, nil
}

// Poll gathers the current state of every VM.
func (s *Source) Poll() (model.Snapshot, error) {
	start := time.Now()

	conn, err := s.connect()
	if err != nil {
		return model.Snapshot{}, err
	}

	domains, _, err := conn.ConnectListAllDomains(1, 0)
	if err != nil {
		// Almost always means the connection died under us. Drop it so the
		// next poll redials rather than retrying on a corpse.
		s.Close()
		return model.Snapshot{}, fmt.Errorf("list domains: %w", err)
	}

	vms := make([]model.VM, len(domains))
	sem := make(chan struct{}, s.cfg.Concurrency)
	var wg sync.WaitGroup

	for i, d := range domains {
		wg.Add(1)
		go func(i int, d libvirt.Domain) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			vms[i] = s.inspect(conn, d)
		}(i, d)
	}
	wg.Wait()

	withAgent := 0
	for _, v := range vms {
		if v.Agent == model.AgentOK {
			withAgent++
		}
	}

	took := time.Since(start)
	s.log.Info("poll ok",
		"vms", len(vms),
		"with_agent", withAgent,
		"took", took.Round(time.Millisecond),
	)

	return model.Snapshot{
		VMs:       vms,
		Polled:    time.Now(),
		PollMS:    took.Milliseconds(),
		Connected: true,
	}, nil
}

// inspect gathers everything about one domain. It never returns an error —
// a VM we can't fully read still gets a row with whatever we did manage,
// because "this VM exists and is running but the agent is quiet" is useful
// information, not a failure.
func (s *Source) inspect(conn *libvirt.Libvirt, d libvirt.Domain) model.VM {
	vm := model.VM{
		Domain:      d.Name,
		Name:        d.Name,
		Interfaces:  []model.Iface{},
		Filesystems: []model.Filesystem{},
		Updated:     time.Now(),
	}

	// TrueNAS names domains "<vm_id>_<vm_name>". Split it so the UI can show
	// "test" rather than "12_test". Note this id is TrueNAS's, and is not the
	// same number as libvirt's runtime d.ID.
	if m := domainNameRE.FindStringSubmatch(d.Name); m != nil {
		if id, err := strconv.Atoi(m[1]); err == nil {
			vm.ID = id
			vm.Name = m[2]
		}
	}

	state, maxMem, _, vcpus, cpuTime, err := conn.DomainGetInfo(d)
	if err != nil {
		s.log.Warn("domain info failed", "domain", d.Name, "err", err)
		vm.State = "unknown"
		return vm
	}

	vm.State = stateName(state)
	vm.Running = libvirt.DomainState(state) == libvirt.DomainRunning
	vm.VCPUs = int(vcpus)
	vm.MemTotalKiB = maxMem

	if !vm.Running {
		// A stopped VM has nothing else to tell us, but it still belongs in
		// the list — vanishing when you shut one down is exactly the thing
		// that makes an in-guest monitoring tool useless for this job.
		s.forgetCPU(d.Name)
		vm.Agent = model.AgentAbsent
		return vm
	}

	vm.CPUPercent, vm.CPUKnown = s.cpuPercent(d.Name, cpuTime, int(vcpus))

	if used, total, ok := s.memory(conn, d); ok {
		vm.MemUsedKiB = used
		vm.MemKnown = true
		if total > 0 {
			vm.MemTotalKiB = total
		}
	}

	// Read the channel state out of the XML before calling the agent. A VM
	// without qemu-guest-agent installed shows state='disconnected', and
	// skipping it here is the difference between a poll that costs nothing
	// and one that burns the full agent timeout on every agentless VM.
	xmlDesc, err := conn.DomainGetXMLDesc(d, 0)
	if err != nil {
		s.log.Warn("domain xml failed", "domain", d.Name, "err", err)
		vm.Agent = model.AgentError
		vm.AgentError = err.Error()
		return vm
	}

	uuid, channelState := parseDomainXML(xmlDesc)
	vm.UUID = uuid

	switch channelState {
	case "":
		vm.Agent = model.AgentAbsent
	case "connected":
		s.fillFromAgent(conn, d, &vm)
	default:
		vm.Agent = model.AgentDisconnected
	}

	s.logAgentTransition(d.Name, vm.Agent)
	return vm
}

// fillFromAgent runs the guest agent queries. Each one is best-effort: a
// guest that answers guest-get-osinfo but not guest-get-fsinfo should still
// show its OS.
func (s *Source) fillFromAgent(conn *libvirt.Libvirt, d libvirt.Domain, vm *model.VM) {
	call := s.agentCaller(conn, d)

	if err := agent.Ping(call); err != nil {
		vm.Agent = model.AgentError
		vm.AgentError = err.Error()
		return
	}
	vm.Agent = model.AgentOK

	if host, err := agent.Hostname(call); err == nil {
		vm.Hostname = host
	} else {
		s.log.Debug("hostname failed", "domain", d.Name, "err", err)
	}

	if osName, kernel, err := agent.OSInfo(call); err == nil {
		vm.OS, vm.Kernel = osName, kernel
	} else {
		s.log.Debug("osinfo failed", "domain", d.Name, "err", err)
	}

	if ifaces, err := agent.Interfaces(call); err == nil {
		vm.Interfaces = ifaces
	} else {
		s.log.Debug("interfaces failed", "domain", d.Name, "err", err)
	}

	if fs, err := agent.Filesystems(call); err == nil {
		vm.Filesystems = fs
	} else {
		s.log.Debug("fsinfo failed", "domain", d.Name, "err", err)
	}
}

// agentCaller adapts libvirt's agent RPC to the agent package's Caller.
func (s *Source) agentCaller(conn *libvirt.Libvirt, d libvirt.Domain) agent.Caller {
	return func(cmd string) (string, error) {
		res, err := conn.QEMUDomainAgentCommand(d, cmd, s.cfg.AgentTimeout, 0)
		if err != nil {
			return "", err
		}
		if len(res) == 0 {
			return "", fmt.Errorf("empty agent reply")
		}
		return res[0], nil
	}
}

// memory reads guest memory from the virtio balloon.
//
// used is worked out as total minus MemAvailable where the guest reports it,
// which is what `free` calls used. Falling back to total minus MemFree counts
// the page cache as used and makes every healthy Linux box look full.
func (s *Source) memory(conn *libvirt.Libvirt, d libvirt.Domain) (used, total uint64, ok bool) {
	stats, err := conn.DomainMemoryStats(d, 16, 0)
	if err != nil {
		s.log.Debug("memory stats failed", "domain", d.Name, "err", err)
		return 0, 0, false
	}

	var available, unused, usable uint64
	var haveAvailable, haveUnused, haveUsable bool
	for _, st := range stats {
		switch st.Tag {
		case memStatAvailable:
			available, haveAvailable = st.Val, true
		case memStatUnused:
			unused, haveUnused = st.Val, true
		case memStatUsable:
			usable, haveUsable = st.Val, true
		}
	}
	if !haveAvailable {
		return 0, 0, false
	}

	switch {
	case haveUsable && usable <= available:
		return available - usable, available, true
	case haveUnused && unused <= available:
		return available - unused, available, true
	default:
		return 0, 0, false
	}
}

// cpuPercent turns libvirt's cumulative CPU nanoseconds into a percentage of
// the VM's allocated cores. Returns ok=false on the first poll for a domain,
// when there's nothing to compare against yet.
func (s *Source) cpuPercent(domain string, cpuTime uint64, vcpus int) (float64, bool) {
	now := time.Now()

	s.mu.Lock()
	prev, had := s.prevCPU[domain]
	s.prevCPU[domain] = cpuSample{cpuTime: cpuTime, at: now}
	s.mu.Unlock()

	if !had || vcpus <= 0 {
		return 0, false
	}
	elapsed := now.Sub(prev.at).Nanoseconds()
	if elapsed <= 0 || cpuTime < prev.cpuTime {
		// Counter went backwards, so the VM restarted. Skip this sample.
		return 0, false
	}

	pct := float64(cpuTime-prev.cpuTime) / float64(elapsed) * 100 / float64(vcpus)
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	return pct, true
}

func (s *Source) forgetCPU(domain string) {
	s.mu.Lock()
	delete(s.prevCPU, domain)
	s.mu.Unlock()
}

// logAgentTransition logs only when a VM's agent state changes, so a box
// without the agent installed doesn't write a line every single poll.
func (s *Source) logAgentTransition(domain string, now model.AgentState) {
	s.mu.Lock()
	prev, had := s.lastAgent[domain]
	s.lastAgent[domain] = now
	s.mu.Unlock()

	if had && prev == now {
		return
	}
	switch {
	case now == model.AgentOK && had:
		s.log.Info("guest agent back", "domain", domain, "was", prev)
	case now == model.AgentOK:
		s.log.Info("guest agent responding", "domain", domain)
	case now == model.AgentDisconnected:
		s.log.Warn("guest agent not running in guest", "domain", domain,
			"hint", "apt install qemu-guest-agent")
	case now == model.AgentError:
		s.log.Warn("guest agent channel open but not answering", "domain", domain)
	}
}

// domainXML is the slice of a domain's XML we actually care about.
type domainXML struct {
	XMLName xml.Name `xml:"domain"`
	UUID    string   `xml:"uuid"`
	Devices struct {
		Channels []struct {
			Target struct {
				Type  string `xml:"type,attr"`
				Name  string `xml:"name,attr"`
				State string `xml:"state,attr"`
			} `xml:"target"`
		} `xml:"channel"`
	} `xml:"devices"`
}

// parseDomainXML pulls the UUID and the guest agent channel's state. An empty
// channel state means the VM has no guest agent channel at all.
func parseDomainXML(raw string) (uuid, channelState string) {
	var dx domainXML
	if err := xml.Unmarshal([]byte(raw), &dx); err != nil {
		return "", ""
	}
	for _, ch := range dx.Devices.Channels {
		if ch.Target.Name == guestAgentChannel {
			return dx.UUID, ch.Target.State
		}
	}
	return dx.UUID, ""
}

func stateName(state uint8) string {
	switch libvirt.DomainState(state) {
	case libvirt.DomainRunning:
		return "running"
	case libvirt.DomainBlocked:
		return "blocked"
	case libvirt.DomainPaused:
		return "paused"
	case libvirt.DomainShutdown:
		return "shutting down"
	case libvirt.DomainShutoff:
		return "stopped"
	case libvirt.DomainCrashed:
		return "crashed"
	case libvirt.DomainPmsuspended:
		return "suspended"
	default:
		return "unknown"
	}
}
