package paxos

import (
	"strconv"
	"fmt"
	)

type AcceptorMsg struct {
	Action string
	Id int
	Ballot int
	Value int //-1 means null
}

type AcceptedRecord struct {
	Id int
	Promised PromiseRecord
}

type PromiseRecord struct {
	Ballot int
	Value ProposalValue //-1 means null
}

type Acceptor struct {
	Owner *PaxosAgent
	AcceptedRecords []AcceptedRecord
}

func (acceptor *Acceptor) AcceptorLoop() {
	for {
		select{
			case msg := <- acceptor.Owner.AcceptChan:{
				switch msg.Action {
				case "prepare":
					
					promised := acceptor.getInstanceStatus(msg.Id)
					if promised == nil {
						//new instance
						acceptor.acceptorLog("Promising for Id: " + strconv.Itoa(msg.Id) + " Ballot N: " + strconv.Itoa(msg.Ballot))
						tmpv := ProposalValue {
							Ballot: -1,
							Val: -1,
						}
						promised = &PromiseRecord {
							Ballot: msg.Ballot,
							Value: tmpv,
						}
						acceptedRecord := AcceptedRecord{
							Id: msg.Id,
							Promised: *promised,
						}
						acceptor.AcceptedRecords = append(acceptor.AcceptedRecords, acceptedRecord)
						//boardcast promise to proposal chan
						promiseValue := ProposalValue {
							Val: -1,
						}
						promise := &Proposal {
							Action: "promise",
							Id: msg.Id,
							Ballot: msg.Ballot,
							Value: promiseValue,
							AcceptorId: acceptor.Owner.Id,
						}
						BroadcastProposeChan(promise)
					} else {
						if msg.Ballot > promised.Ballot {
							acceptor.updateAcceptAfterPromise(msg.Id, msg.Ballot)
							acceptor.acceptorLog("Promising for Id: " + strconv.Itoa(msg.Id) + " Ballot N: " + strconv.Itoa(msg.Ballot) + " Value: " + strconv.Itoa(promised.Value.Val))
							//boardcast promise to proposal chan
							promise := &Proposal {
								Action: "promise",
								Id: msg.Id,
								Ballot: msg.Ballot,
								Value: promised.Value,
								AcceptorId: acceptor.Owner.Id,
							}
							BroadcastProposeChan(promise)
						} else {
							acceptor.acceptorLog("Refusing to promise for Id: " + strconv.Itoa(msg.Id) + " Ballot N: " + strconv.Itoa(msg.Ballot))
						}
					}
				case "accept":
					promised := acceptor.getInstanceStatus(msg.Id)
					if promised == nil {
						// new instance.
						acceptor.acceptorLogF("Accepting value: %d for id: %d", msg.Value, msg.Id)
						tmpv := ProposalValue {
							Ballot: msg.Ballot,
							Val: msg.Value,
						}
						promised = &PromiseRecord {
							Ballot: -1,
							Value: tmpv,
						}
						acceptedRecord := AcceptedRecord{
							Id: msg.Id,
							Promised: *promised,
						}
						acceptor.AcceptedRecords = append(acceptor.AcceptedRecords, acceptedRecord)
						//boardcast learn chan
						acceptedMsg := &LearnerMsg{
							Action: "value_accepted",
							Id: msg.Id,
							Ballot: msg.Ballot,
							Value: tmpv,
							AcceptorId: acceptor.Owner.Id,
						}
						BroadcastLearnChan(acceptedMsg)
					} else {
						if msg.Ballot >= promised.Ballot {
							acceptor.acceptorLogF("Accepting value: %d for id: %d ballot %d is bigger than last promised", msg.Value, msg.Id, msg.Ballot)
							//boardcast learn chan 
							acceptor.updateAcceptAfterAccept(msg.Id, msg.Ballot, msg.Value)
							v := ProposalValue {
								Ballot: -1,
								Val: msg.Value,
							}
							acceptedMsg := &LearnerMsg{
								Action: "value_accepted",
								Id: msg.Id,
								Ballot: msg.Ballot,
								Value: v,
								AcceptorId: acceptor.Owner.Id,
							}
							BroadcastLearnChan(acceptedMsg)
						} else {
							acceptor.acceptorLogF("Refusing to accept: %d for id: %d, already promised %d", msg.Value, msg.Id, promised.Ballot)
						}
					}
					
				}
			}
		}
	}
}

func (acceptor *Acceptor) updateAcceptAfterPromise(id, new_ballot int) {
	for i := 0; i < len(acceptor.AcceptedRecords); i++ {
		if id == acceptor.AcceptedRecords[i].Id {
			acceptor.AcceptedRecords[i].Promised.Ballot = new_ballot
		}
	}
}

func (acceptor *Acceptor) updateAcceptAfterAccept(id, ballot, val int) {
	for i := 0; i < len(acceptor.AcceptedRecords); i++ {
		if id == acceptor.AcceptedRecords[i].Id {
			acceptor.AcceptedRecords[i].Promised.Value = ProposalValue{
				Ballot: ballot,
				Val: val,
			}
		}
	}
}

func (acceptor *Acceptor) getInstanceStatus(id int) (promised *PromiseRecord) {
	// promised == nil: new instance.
	promised = nil
	if len(acceptor.AcceptedRecords) == 0 {
		return
	}
	for i := 0; i < len(acceptor.AcceptedRecords); i++ {
		if id == acceptor.AcceptedRecords[i].Id {
			promised = &acceptor.AcceptedRecords[i].Promised
			return
		}
	}
	return
}
func (acceptor *Acceptor) acceptorLogF(format string, a ...interface{}) {
	acceptor.acceptorLog(fmt.Sprintf(format, a ...))
}

func (acceptor *Acceptor) acceptorLog(msg string) {
	acceptor.Owner.LogInfo("Acceptor: " + msg)
}