// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"time"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1alpha3"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1beta1"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// needsRemediation reports whether a MachineHealthCheck flagged the machine as unhealthy and left its
// remediation to the control plane (MachineHealthCheckSucceeded=False and MachineOwnerRemediated=False).
// Both conditions are required: when the machine recovers on its own the MachineHealthCheck controller
// sets MachineHealthCheckSucceeded back to True but leaves MachineOwnerRemediated to the owner, so a
// check on MachineOwnerRemediated alone would treat the machine as unhealthy forever.
func needsRemediation(machine *clusterv1.Machine) bool {
	return collections.IsUnhealthyAndOwnerRemediated(machine)
}

// remainingMachines returns the owned machines that are staying in the control plane: those that are
// not being deleted, not leaving etcd (see etcdLeavingAnnotation), and not flagged for remediation by
// a MachineHealthCheck (see needsRemediation). A machine in any of these states is on its way out, so
// its stopped or already-removed etcd must not keep EtcdClusterHealthyCondition false and deadlock
// scale-down, rollout, and remediation. For the same reason the boot check before a scale-down only
// waits for these machines.
func remainingMachines(ownedMachines []clusterv1.Machine) []clusterv1.Machine {
	machines := make([]clusterv1.Machine, 0, len(ownedMachines))

	for _, machine := range ownedMachines {
		if machine.ObjectMeta.DeletionTimestamp.IsZero() &&
			machine.Annotations[etcdLeavingAnnotation] != "true" &&
			!needsRemediation(&machine) {
			machines = append(machines, machine)
		}
	}

	return machines
}

// nodeNamesForMachine returns the host names that can identify a machine's etcd member: the noderef
// name and every MachineHostName address (some infrastructure providers report the infrastructure
// machine name there, which may not match the node hostname). Each name is lowercased and has any
// domain suffix trimmed (noderef names can be FQDNs, e.g. on AWS). The caller must ensure NodeRef is set.
func nodeNamesForMachine(machine clusterv1.Machine) []string {
	candidates := []string{machine.Status.NodeRef.Name}

	for _, address := range machine.Status.Addresses {
		if address.Type == clusterv1.MachineHostName {
			candidates = append(candidates, address.Address)
		}
	}

	names := make([]string, 0, len(candidates))

	for _, hostname := range candidates {
		hostname, _, _ = strings.Cut(hostname, ".")
		names = append(names, strings.ToLower(hostname))
	}

	return names
}

// etcdMemberNames returns the set of host names (see nodeNamesForMachine) an etcd member may carry to
// belong to one of the machines. Machines without a noderef have no known names and contribute nothing.
func etcdMemberNames(machines []clusterv1.Machine) map[string]struct{} {
	names := map[string]struct{}{}

	for _, machine := range machines {
		if machine.Status.NodeRef == nil {
			continue
		}

		for _, name := range nodeNamesForMachine(machine) {
			names[name] = struct{}{}
		}
	}

	return names
}

// machineAddresses returns the addresses of a machine that an etcd member's peer URL can point to:
// every status address except MachineHostName, which nodeNamesForMachine already covers, normalized
// like etcdPeerHosts.
func machineAddresses(machine clusterv1.Machine) []string {
	addresses := make([]string, 0, len(machine.Status.Addresses))

	for _, address := range machine.Status.Addresses {
		if address.Type == clusterv1.MachineHostName {
			continue
		}

		addresses = append(addresses, normalizeAddress(address.Address))
	}

	return addresses
}

// etcdMemberAddresses returns the set of addresses (see machineAddresses) an etcd member's peer URL may
// point to to belong to one of the machines. Like etcdMemberNames, machines without a noderef
// contribute nothing.
func etcdMemberAddresses(machines []clusterv1.Machine) map[string]struct{} {
	addresses := map[string]struct{}{}

	for _, machine := range machines {
		if machine.Status.NodeRef == nil {
			continue
		}

		for _, address := range machineAddresses(machine) {
			addresses[address] = struct{}{}
		}
	}

	return addresses
}

// etcdPeerHosts returns the hosts of an etcd member's peer URLs, normalized like machineAddresses.
//
// A member keeps the host name the node had when it joined etcd. On some platforms (e.g. Hetzner
// Cloud) that is still the name Talos generates (talos-xxx-xxx), and the node only gets its final
// name afterwards, so the member name never matches the machine. The peer URL carries the node's
// address, which does.
func etcdPeerHosts(member *machineapi.EtcdMember) []string {
	hosts := make([]string, 0, len(member.PeerUrls))

	for _, peerURL := range member.PeerUrls {
		u, err := url.Parse(peerURL)
		if err != nil || u.Hostname() == "" {
			continue
		}

		hosts = append(hosts, normalizeAddress(u.Hostname()))
	}

	return hosts
}

// normalizeAddress lowercases an address and puts an IP into its canonical form, so that the same
// address written two ways (e.g. a full and a compressed IPv6 address) compares equal.
func normalizeAddress(address string) string {
	if ip := net.ParseIP(address); ip != nil {
		return ip.String()
	}

	return strings.ToLower(address)
}

// memberMatches reports whether an etcd member belongs to a machine with the given host names and
// addresses: by its host name, or by the host of one of its peer URLs (see etcdPeerHosts).
func memberMatches(member *machineapi.EtcdMember, names, addresses map[string]struct{}) bool {
	if _, ok := names[strings.ToLower(member.Hostname)]; ok {
		return true
	}

	for _, host := range etcdPeerHosts(member) {
		if _, ok := addresses[host]; ok {
			return true
		}
	}

	return false
}

// toSet returns the elements of values as a set.
func toSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))

	for _, value := range values {
		set[value] = struct{}{}
	}

	return set
}

// etcdMachineIdentity holds what identifies a machine's etcd member: its host names (see
// nodeNamesForMachine) and its addresses (see machineAddresses).
type etcdMachineIdentity struct {
	names     map[string]struct{}
	addresses map[string]struct{}
}

// etcdMembership holds what an etcd member list is verified against: the host names of the machines
// that must be etcd members (expected, keyed by machine name), the machines that must be members but
// whose names are not known yet because they have no noderef (pending), and the host names and
// addresses of every owned machine (owned, ownedAddresses), which decide whether a member is an orphan.
type etcdMembership struct {
	expected       map[string]etcdMachineIdentity
	pending        []string
	owned          map[string]struct{}
	ownedAddresses map[string]struct{}
}

// newEtcdMembership builds the membership for the owned machines, of which machines is the subset
// that takes part in the etcd health check (see remainingMachines).
func newEtcdMembership(ownedMachines, machines []clusterv1.Machine) etcdMembership {
	membership := etcdMembership{
		expected:       make(map[string]etcdMachineIdentity, len(machines)),
		owned:          etcdMemberNames(ownedMachines),
		ownedAddresses: etcdMemberAddresses(ownedMachines),
	}

	for _, machine := range machines {
		if machine.Status.NodeRef == nil {
			membership.pending = append(membership.pending, machine.Name)

			continue
		}

		membership.expected[machine.Name] = etcdMachineIdentity{
			names:     toSet(nodeNamesForMachine(machine)),
			addresses: toSet(machineAddresses(machine)),
		}
	}

	return membership
}

// verify checks the etcd member list reported by node and returns the set of member host names,
// lowercased, for comparison across nodes.
//
// Every machine that must be a member has to have one, matched by any of its host names or by the
// address in the member's peer URL (see memberMatches). A machine
// that must be a member but has no noderef yet (it is new and the kubelet has not registered) cannot
// be matched by name, so it is accounted for by count: each such machine claims one member matching
// no owned machine. Any further unmatched member is an orphan (auditEtcd force-removes these), and
// fewer unmatched members than pending machines means a new machine has not joined etcd yet. A member
// of an excluded machine still matches an owned machine, so it is tolerated; that machine's member
// may also already be gone, which is expected, so it is not required.
func (m etcdMembership) verify(node string, members []*machineapi.EtcdMember) (map[string]struct{}, error) {
	present := make(map[string]struct{}, len(members))

	var unmatched []string

	for _, member := range members {
		present[strings.ToLower(member.Hostname)] = struct{}{}

		if !memberMatches(member, m.owned, m.ownedAddresses) {
			unmatched = append(unmatched, member.Hostname)
		}
	}

	for machineName, identity := range m.expected {
		if !slices.ContainsFunc(members, func(member *machineapi.EtcdMember) bool {
			return memberMatches(member, identity.names, identity.addresses)
		}) {
			return nil, fmt.Errorf("%s: etcd is missing a member for control plane machine %q", node, machineName)
		}
	}

	switch {
	case len(unmatched) > len(m.pending):
		return nil, fmt.Errorf("%s: etcd member %q does not match any control plane machine", node, unmatched[len(m.pending)])
	case len(unmatched) < len(m.pending):
		return nil, fmt.Errorf("%s: etcd is missing a member for %d of the control plane machines without a noderef yet %q",
			node, len(m.pending)-len(unmatched), m.pending)
	}

	return present, nil
}

// compareEtcdMembers reports a member list that differs from the one another node reported: each etcd
// member must see the same cluster, so a member known to only one of the two nodes is an error.
func compareEtcdMembers(node string, members map[string]struct{}, previousNode string, previous map[string]struct{}) error {
	for name := range members {
		if _, ok := previous[name]; !ok {
			return fmt.Errorf("%s: etcd member %q is not known to %s", node, name, previousNode)
		}
	}

	for name := range previous {
		if _, ok := members[name]; !ok {
			return fmt.Errorf("%s: etcd member %q known to %s is missing", node, name, previousNode)
		}
	}

	return nil
}

func (r *TalosControlPlaneReconciler) etcdHealthcheck(ctx context.Context, tcp *controlplanev1.TalosControlPlane, ownedMachines []clusterv1.Machine) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	machines := remainingMachines(ownedMachines)

	// If every owned machine is on its way out (deleting, leaving, or being remediated) there is
	// nothing left to verify. Reporting etcd healthy here would open the scale-down/rollout gate
	// without a single member checked, so treat it as unhealthy until a machine is back in the set.
	if len(ownedMachines) > 0 && len(machines) == 0 {
		return fmt.Errorf("all %d owned control plane machines are excluded from the etcd health check", len(ownedMachines))
	}

	membership := newEtcdMembership(ownedMachines, machines)

	params := make([]any, 0, len(machines)*2)
	for _, machine := range machines {
		params = append(params, "node", machine.Name)
	}

	r.Log.Info("verifying etcd health on all nodes", params...)

	const service = "etcd"

	// the member list reported by the previous node, to check that every node sees the same cluster
	var (
		previousNode    string
		previousMembers map[string]struct{}
	)

	for _, machine := range machines {
		// loop for each machine, the client created has endpoints which point to a single machine
		if err := func() error {
			c, err := r.talosconfigForMachines(ctx, tcp, machine)
			if err != nil {
				return err
			}

			defer c.Close() //nolint:errcheck

			svcs, err := c.ServiceInfo(ctx, service)
			if err != nil {
				return err
			}

			// check that etcd service is healthy on the node
			for _, svc := range svcs {
				node := svc.Metadata.GetHostname()

				if len(svc.Service.Events.Events) == 0 {
					return fmt.Errorf("%s: no events recorded yet for service %q", node, service)
				}

				lastEvent := svc.Service.Events.Events[len(svc.Service.Events.Events)-1]
				if lastEvent.State != "Running" {
					return fmt.Errorf("%s: service %q not in expected state %q: current state [%s] %s", node, service, "Running", lastEvent.State, lastEvent.Msg)
				}

				if !svc.Service.GetHealth().GetHealthy() {
					return fmt.Errorf("%s: service is not healthy: %s", node, service)
				}
			}

			resp, err := c.EtcdMemberList(ctx, &machineapi.EtcdMemberListRequest{})
			if err != nil {
				return err
			}

			for _, message := range resp.Messages {
				node := message.Metadata.GetHostname()

				members, err := membership.verify(node, message.Members)
				if err != nil {
					return err
				}

				if previousMembers != nil {
					if err := compareEtcdMembers(node, members, previousNode, previousMembers); err != nil {
						return err
					}
				}

				previousNode, previousMembers = node, members
			}

			return nil
		}(); err != nil {
			return fmt.Errorf("error checking etcd health on machine %q: %w", machine.Name, err)
		}
	}

	return nil
}

// gracefulEtcdLeave removes a given machine from the etcd cluster by forfeiting leadership
// and issuing a "leave" request from the machine itself.
func (r *TalosControlPlaneReconciler) gracefulEtcdLeave(ctx context.Context, c *talosclient.Client, machineToLeave clusterv1.Machine) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)

	defer cancel()

	r.Log.Info("verifying etcd status", "machine", machineToLeave.Name, "node", machineToLeave.Status.NodeRef.Name)

	svcs, err := c.ServiceInfo(ctx, "etcd")
	if err != nil {
		return err
	}

	for _, svc := range svcs {
		if svc.Service.State != "Finished" {
			r.Log.Info("forfeiting leadership", "machine", machineToLeave.Status.NodeRef.Name)

			_, err = c.EtcdForfeitLeadership(ctx, &machineapi.EtcdForfeitLeadershipRequest{})
			if err != nil {
				return err
			}

			r.Log.Info("leaving etcd", "machine", machineToLeave.Name, "node", machineToLeave.Status.NodeRef.Name)

			err = c.EtcdLeaveCluster(ctx, &machineapi.EtcdLeaveClusterRequest{})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// forceEtcdLeave removes a given machine from the etcd cluster by telling another CP node to remove the member.
// This is used in times when the machine was deleted out from under us.
func (r *TalosControlPlaneReconciler) forceEtcdLeave(ctx context.Context, c *talosclient.Client, member *machineapi.EtcdMember) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)

	defer cancel()

	r.Log.Info("removing etcd member", "memberName", member.Hostname, "memberId", member.Id)

	return c.EtcdRemoveMemberByID(
		ctx,
		&machineapi.EtcdRemoveMemberByIDRequest{
			MemberId: member.Id,
		},
	)
}

// auditEtcd rolls through all etcd members to see if there's a matching controlplane machine
// It uses the first controlplane node returned as the etcd endpoint
func (r *TalosControlPlaneReconciler) auditEtcd(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster client.ObjectKey) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)

	defer cancel()

	machines, err := r.getControlPlaneMachinesForCluster(ctx, cluster)
	if err != nil {
		return err
	}

	if len(machines.Items) == 0 {
		return nil
	}

	for _, machine := range machines.Items {
		// nb: we'll assume any machine that doesn't have a noderef is new and we can audit later because
		//     otherwise a new etcd member can get removed before even getting the noderef set by the CAPI controllers.
		if machine.Status.NodeRef == nil {
			return fmt.Errorf("some CP machines do not have a noderef")
		}
	}
	// Select the first CP machine that's not being deleted and has a noderef
	var designatedCPMachine clusterv1.Machine

	for _, machine := range machines.Items {
		if !machine.ObjectMeta.DeletionTimestamp.IsZero() || machine.Status.NodeRef == nil {
			continue
		}

		designatedCPMachine = machine

		break
	}

	if designatedCPMachine.Name == "" {
		return fmt.Errorf("no CP machine which is not being deleted and has node ref")
	}

	c, err := r.talosconfigForMachines(ctx, tcp, designatedCPMachine)
	if err != nil {
		return err
	}

	defer c.Close() //nolint:errcheck

	response, err := c.EtcdMemberList(ctx, &machineapi.EtcdMemberListRequest{})
	if err != nil {
		return fmt.Errorf("error getting etcd members via %q (endpoints %v): %w", designatedCPMachine.Name, c.GetConfigContext().Endpoints, err)
	}

	// Only querying one CP node, so only 1 message should return.
	memberList := response.Messages[0]

	// every member must belong to one of the machines, by host name or peer URL address, matched the same
	// way the health check does
	names := etcdMemberNames(machines.Items)
	addresses := etcdMemberAddresses(machines.Items)

	for _, member := range memberList.Members {
		if member.Hostname == "" {
			return fmt.Errorf("discovered etcd member with empty hostname: %s", member)
		}

		if !memberMatches(member, names, addresses) {
			r.Log.Info("found etcd member that doesn't exist as controlplane machine", "member", member)

			if err = r.forceEtcdLeave(ctx, c, member); err != nil {
				return fmt.Errorf("error leaving etcd for member %q via machine %q", member, designatedCPMachine.Name)
			}
		}
	}

	return nil
}
