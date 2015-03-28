package paxos

import (
	"strconv"
	"time"
	//"fmt"
	)

type InstRecord struct {
	ProposalId int
	BallotN int
}

type ProposalValue struct {
	Ballot int
	Val int
}

type Proposal struct {
	Action string
	Id int
	Ballot int
	Value ProposalValue
	AcceptorId int
}

type Phase1Record struct{
	WaitingPromise []ProposalPromise //AcceptorId should be -1
	PromiseQueue []ProposalPromise
}

type ProposalPromise struct {
	Id int
	Ballot int
	Value ProposalValue
	AcceptorId int
}

type Proposer struct {
	Owner *PaxosAgent
	ProposalId int
	Majority int
	InstRecords []InstRecord
	Phase1Records Phase1Record
}

func (proposer *Proposer)ProposerLoop() {
	//proposerloop:
	for {
		select {
			case msg := <- proposer.Owner.ProposeChan:{
				switch msg.Action {
				case "start_new_proposal":
					proposer.proposerLog(" start a proposal: " + strconv.Itoa(msg.Value.Val))
					proposer.propose(msg.Value.Val)
				case "promise":
					//proposer.proposerLog(" receive a promise!" + strconv.Itoa(msg.Value.Val))
					isRelevant := proposer.isPromiseRelevant(msg.Id, msg.Ballot, msg.Value, msg.AcceptorId)
					count := 0
					if isRelevant {
						proposer.enqueuePromise(msg.Id, msg.Ballot, msg.Value, msg.AcceptorId)
						count = proposer.countPromises(msg.Id, msg.Ballot)
						proposer.proposerLog(" New Promise!" + strconv.Itoa(count))
						if count < proposer.Majority {
							proposer.proposerLog(" Not enough promises so far for id: " + strconv.Itoa(msg.Id))
						} else {
							proposer.proposerLog(" Got a majority of promises for id: " + strconv.Itoa(msg.Id) + " and ballot: " + strconv.Itoa(msg.Ballot))
							val := proposer.removeFromWaiting(msg.Id, msg.Ballot)
							// process promise
							proposer.proposerLog(" Process Promise val: " + strconv.Itoa(val.Val))
							inf := &Proposal{
								Action: "process_promises",
								Id: msg.Id,
								Ballot: msg.Ballot,
								Value: val,
							}
							proposer.Owner.ProposeChan <- *inf
						}
					} else {
						proposer.proposerLog(" Discarded unrelevant promise!")
					}
				case "process_promises":
					proposer.proposerLog(" Processing Promise!")
					relev_promises := proposer.filterOutPromises(msg.Id, msg.Ballot)
					condStr, value := proposer.processPromises(relev_promises)
					proposer.proposerLog("condStr: " + condStr + " value: " + strconv.Itoa(value))
					switch condStr{
					case "value_not_chosen":
						if value == -1 {
							proposer.proposerLog("We promises with no previous value, sending accept " + strconv.Itoa(msg.Value.Val) + " for id " + strconv.Itoa(msg.Id))
							//boardcase accept message
							acceptMsg := &AcceptorMsg{
								Action: "accept",
								Id: msg.Id,
								Ballot: msg.Ballot,
								Value: msg.Value.Val,
							}
							BroadcastAcceptChan(acceptMsg)
						} else {
							if value == msg.Value.Val {
								proposer.proposerLog("Now majority MyVal is best candidate, sending accept " + strconv.Itoa(msg.Value.Val) + " for id " + strconv.Itoa(msg.Id))
								//boardcase accept message
							} else {
								proposer.proposerLog("Somebody else value is best candidate, sending accept "+ strconv.Itoa(msg.Value.Val) + " for id " + strconv.Itoa(msg.Id))
								proposer.proposerLog("... still need to propose " + strconv.Itoa(msg.Value.Val) + " Start Over!")
								//boardcase accept message
								// re-propose myvalue
							}
						}
					case "value_chosen":
						if value == msg.Value.Val {
							proposer.proposerLog(" My Value " + strconv.Itoa(value) + " has been chosen in instance, id: " + strconv.Itoa(msg.Id) + " Ballot: " + strconv.Itoa(msg.Ballot))
							// boardcast learn message
						} else {
							proposer.proposerLog(" Another Value " + strconv.Itoa(value) + " has been chosen already for instance, id: " + strconv.Itoa(msg.Id) + " Ballot: " + strconv.Itoa(msg.Ballot))
							// re-propose myvalue
						}
					}
					
				case "promise_collection_timeout":
					//TODO
					proposer.proposerLog(" Promise Collection Time out!")
				case "verification_timeout":
					//TODO
			    case "new_majority":
					proposer.Majority = msg.Value.Val
					//proposer.proposerLog(" set new majority:" + strconv.Itoa(proposer.Majority))
				//default:
				//	proposer.proposerLog("Unknown Message")
				}
			}
		}
	}
}

func (proposer *Proposer) propose(val int) {
	id := proposer.getNewIdAndUpdate()
	ballot := proposer.getNextBallotNumberAndUpdate(id)
	proposer.proposerLog(" ProposalId: " + strconv.Itoa(id) + " Ballot N: " + strconv.Itoa(ballot))
	proposer.setCollectPromiseTimeout(id, ballot, val)
	msg := &AcceptorMsg{
		Action: "prepare",
		Id: id,
		Ballot: ballot,
		Value: -1, //-1 means null
	}
	BroadcastAcceptChan(msg)
}

func (proposer *Proposer) filterOutPromises(id, ballot int) []ProposalPromise {
	relevant := make([]ProposalPromise,0)
	for i := 0; i < len(proposer.Phase1Records.PromiseQueue); i++ {
		if proposer.Phase1Records.PromiseQueue[i].Id == id &&
			proposer.Phase1Records.PromiseQueue[i].Ballot == ballot {
				proposer.proposerLog("filter out promise val: " + strconv.Itoa(proposer.Phase1Records.PromiseQueue[i].Value.Val))
				relevant = append(relevant, proposer.Phase1Records.PromiseQueue[i])
		}
	}
	return relevant
}

func (proposer *Proposer) processPromises(promises []ProposalPromise) (string, int) {
	values := getValuesSet(promises)
	if len(values) == 1 {
		for k,v := range values {
			if v && k != -1 {
				return "value_chosen", k
			} else {
				return "value_not_chosen", -1
			}
		}
	}
	return "value_not_chosen", bestValueCandidateInPromises(promises)
}

func bestValueCandidateInPromises(promises []ProposalPromise) int {
	value := ProposalValue {
		Ballot: -1,
		Val: -1,
	}
	for i := 0; i < len(promises); i++ {
		
		if value.Ballot == -1 && promises[i].Value.Val != -1 {
			value = promises[i].Value
		}
		if value.Ballot != -1 && promises[i].Value.Ballot > value.Ballot {
			value = promises[i].Value
		}
	}
	
	return value.Val
}

func getValuesSet(promises []ProposalPromise) map[int]bool {
	values := make(map[int]bool)
	for i := 0; i < len(promises); i++ {
		_, ok := values[promises[i].Value.Val]
		if ! ok {
			values[promises[i].Value.Val] = true
		}
	}
	return values
}

func (proposer *Proposer) enqueuePromise(id, ballot int, value ProposalValue, acceptorId int) {
	promise := ProposalPromise {
		id,
		ballot,
		value,
		acceptorId,
	}
	proposer.Phase1Records.PromiseQueue = append(proposer.Phase1Records.PromiseQueue, promise)
}

func (proposer *Proposer) removeFromWaiting(id, ballot int) ProposalValue {
	
	i := 0
	for  ;i < len(proposer.Phase1Records.WaitingPromise); i++ {
		if proposer.Phase1Records.WaitingPromise[i].Id == id &&
			proposer.Phase1Records.WaitingPromise[i].Ballot == ballot {
				break
		}
	}
	val := proposer.Phase1Records.WaitingPromise[i].Value
	proposer.proposerLog("WaitingPromise len: " + strconv.Itoa(len(proposer.Phase1Records.WaitingPromise)))
	proposer.Phase1Records.WaitingPromise = append(proposer.Phase1Records.WaitingPromise[:i], proposer.Phase1Records.WaitingPromise[i+1:]...)
	proposer.proposerLog("WaitingPromise len: " + strconv.Itoa(len(proposer.Phase1Records.WaitingPromise)))
	return val
}

func (proposer *Proposer) countPromises(id, ballot int) int {
	count := 0
	for i := 0; i < len(proposer.Phase1Records.PromiseQueue); i++ {
		if proposer.Phase1Records.PromiseQueue[i].Id == id &&
			proposer.Phase1Records.PromiseQueue[i].Ballot == ballot {
				count++
		}
	}
	return count
}

func (proposer *Proposer) setCollectPromiseTimeout(id, ballot, value int) {
	time.AfterFunc(time.Second * 2, func() {
		valstruct := ProposalValue {
			Val:value,
		}
		msg := &Proposal{
			Action: "promise_collection_timeout",
			Id: id,
			Ballot: ballot,
			Value: valstruct,
		}
		proposer.Owner.ProposeChan <- *msg
	})
	proposer.proposerLog("enqueue waiting queue, value:" + strconv.Itoa(value))
	valstruct := ProposalValue {
		Ballot: ballot,
		Val:value,
	}
	pp := ProposalPromise{
		Id: id,
		Ballot: ballot,
		Value: valstruct,
		AcceptorId: -1,
	}
	proposer.Phase1Records.WaitingPromise = append(proposer.Phase1Records.WaitingPromise, pp)
}

func (proposer *Proposer) isPromiseRelevant(id, ballot int, value ProposalValue, acceptorId int) bool{
	waiting := proposer.isWaitingForPromise(id, ballot)
	if waiting {
		for i := 0; i < len(proposer.Phase1Records.PromiseQueue); i++ {
			if proposer.Phase1Records.PromiseQueue[i].Id == id &&
				proposer.Phase1Records.PromiseQueue[i].Ballot == ballot &&
				proposer.Phase1Records.PromiseQueue[i].Value.Val == value.Val &&
				proposer.Phase1Records.PromiseQueue[i].AcceptorId == acceptorId {
					return false
			}
		}
		return true
	} else {
		return false
	}
}

func (proposer *Proposer) isWaitingForPromise(id, ballot int) bool {
	for i := 0; i < len(proposer.Phase1Records.WaitingPromise); i++ {
		if proposer.Phase1Records.WaitingPromise[i].Id == id &&
			proposer.Phase1Records.WaitingPromise[i].Ballot == ballot {
				return true
		}
	}
	return false
}

func (proposer *Proposer) getNewIdAndUpdate() (id int) {
	id = proposer.ProposalId
	proposer.ProposalId = id + 1
	return
}

func (proposer *Proposer) getNextBallotNumberAndUpdate(pid int) (ballot int) {
	var n int
	if len(proposer.InstRecords) == 0 {
		n = 1
		proposer.InstRecords = append(proposer.InstRecords, InstRecord{pid, n + 1})
	} else {
		for i := 0; i < len(proposer.InstRecords); i++ {
			if pid == proposer.InstRecords[i].ProposalId {
				n = proposer.InstRecords[i].BallotN
				proposer.InstRecords[i].BallotN = n + 1
			}
		}
		if n == 0 {
			n = 1
			proposer.InstRecords = append(proposer.InstRecords, InstRecord{pid, n + 1})
		}
	}
	proposer.proposerLog(" InstRecords length: "+ strconv.Itoa(len(proposer.InstRecords)))
	ballot = insertAgentIdtoBallot(proposer.Owner.Id, n)
	return
}

func (proposer *Proposer) proposerLog(msg string) {
	proposer.Owner.LogInfo("Proposer: " + msg)
}

func insertAgentIdtoBallot(agentId int, n int) (ballot int) {
	ballot = n * MAX_NUM_OF_PROPOSERS + agentId
	return
}