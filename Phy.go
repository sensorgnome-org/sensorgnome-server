package main

import (
	"time"
	"sync"
)

// model of the physical world
//
// Device: a remote machine with a web interface
//
// User: a person who has permission to access some devices;
// identified by email address
//
// Project: a set of Devices and a set of Users
//
// Domain: organization providing users, projects, and memberships
// of users to projects and devices to projects.  Domains can also
// provide downstream handlers for data sync.
//
// Devices exist across Domains, but Projects and Users belong to
// Domains.
//
// - each Device belongs to one or more Projects
// - each User belongs to one or more Projects
// - each User in a Project can access each Device in that Project
// - a Domain can provide metadata about Devices
// - a Domain can indicate that a User or Device belongs to a Project
// - a person can be more than one User, either by having multiple
//   email addresses or by having Users in more than one domain

// unique identifiers across domains:
type DeviceID string // device serial number; e.g. "SG-1234BBBK5678";
type UserID string // looks like "domain:userEmail"
type ProjectID string // looks like "domain:projectName"
type DomainID string // e.g. "motus.org"; must not contain a ":"

// device
type Device struct {
	ID DeviceID
	// for each Domain, a string describing this device (e.g. its deployment location)
	// If a Domain does not supply a description, then the device will be known
	// to that Domain's users by the set of descriptions provided by other domains.
	Desc map[DomainID]string
	// the project(s) the device belongs to; can be from more than one domain
	Projects []*Project
	// time at which connected to server
	TsConn     time.Time
	// time at which last disconnected
	TsDisConn  time.Time
	// record of downstream syncs, by domain
	Syncs map[Domain]*SyncInfo
	// ssh tunnel port, if applicable
	TunnelPort int
	// user directly connected to the devices's web server, if not nil
	WebUser    *User
	// reverse proxy to the device's web server, if not nil
	Proxy      *Proxy
	// actually connected?  once we've seen a device, we keep this
	// struct in memory, even but set this field to false when it
	// disconnects
	Connected  bool
	// lock for any read or write access to fields in this struct
	lock sync.Mutex
}

// authenticated user
type User struct {
	Domain *Domain
	ID UserID
	// which projects the user belongs to
	Projects []*Project
}

// project
type Project struct {
	Domain *Domain
	ID ProjectID
	Desc string
	Devices []*Device
}

// domain in which users and projects exist
type Domain struct {
	ID DomainID
	// create / refresh authenticated user from a set of credentials
	// Implicitly provides the list of projects the user belongs to
	Auth func(creds []string) *User
	// get list of projects, and which devices belong to them
	Projects func() []*Project
	// get information about devices, including sync
	Devices func() []*Devices
	// how often to call Projects(), Devices() to update internal cache
	MaxAge time.Duration()
	// start a goroutine to schedule syncs for a device
	Sync func(Device *)
	// stop any goroutine for scheduling
	StopSync func(Device *)
}

// record of downstream sync events
type SyncInfo struct {
	// time at which last synced with downstream data processor
	TsLastSync time.Time
	// time at which next to be synced with downstream data processor
	TsNextSync time.Time
}

// Phy - the physical model.  Concurrency is handled by locking.

type Phy struct {
	Users map[UserID]User
	Projects map[ProjectID]Project
	Devices map[DeviceID]Device
	Domains map[DomainID]Domain
	// lock
	Lock sync.Mutex
}

// get User from UserID
func (p *Phy) User(u UseriD) *User {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	return Users[u]
}

// get Project from ProjectID
func (p *Phy) User(u UseriD) *User {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	return Users[u]
}
