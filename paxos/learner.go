package paxos

import (
	"fmt"
	)

type LearnerMsg struct {
	Action string
	Id int
	Ballot int
	Value ProposalValue
	AcceptorId int 
}

type LearnedValue struct {
	Id int
	Value ProposalValue
}

type AccMsg struct {
	Ballot int
	Value ProposalValue
	AcceptorId int
}

type InstanceAccMsgs struct {
	Id int
	MsgList []AccMsg
}

type Learner struct {
	Owner *PaxosAgent
	//Learned chan
	Majority int
	LearnedValues []LearnedValue
	//MissingValues
	AccMsgQueue []InstanceAccMsgs
}

func (learner *Learner)LearnerLoop() {
	for {
		select {
			case msg := <- learner.Owner.LearnChan:{
				switch msg.Action {
				case "learn":
					learner.LearnerLog("Receive a learn message!")
					var seen, seenButDiff bool
					for i := 0; i < len(learner.LearnedValues); i++ {
						if msg.Id == learner.LearnedValues[i].Id && msg.Value.Val == learner.LearnedValues[i].Value.Val {
							seen = true
						}
						if msg.Id == learner.LearnedValues[i].Id && msg.Value.Val != learner.LearnedValues[i].Value.Val {
							seenButDiff = true
						}
					}
					if seen {
						learner.LearnerLogF("Ignoring learn for known instance/value: %d/%d", msg.Id, msg.Value.Val)
					} else{
						if seenButDiff {
							learner.LearnerLogF("!!! ERROR !!! learned a different value than %d for instance %d", msg.Id, msg.Value.Val)
						} else {
							learnedValue := LearnedValue{
								Id: msg.Id,
								Value: msg.Value,
							}
							learner.LearnedValues = append(learner.LearnedValues, learnedValue)
							// update missing list
							for i := 0; i < len(learner.AccMsgQueue); i++ {
								if msg.Id == learner.AccMsgQueue[i].Id {
									 learner.AccMsgQueue = append(learner.AccMsgQueue[:i], learner.AccMsgQueue[i+1:]...)
								}
							}
							
							learner.LearnerLogF("learn instance/value: %d/%d", msg.Id, msg.Value.Val)
						}
					}
				case "value_accepted":
					//learner.LearnerLog("Receive a value_accepted message!")
					var alreadyLearned bool
					for i := 0; i < len(learner.LearnedValues); i++ {
						if msg.Id == learner.LearnedValues[i].Id {
							alreadyLearned = true
						}
					}
					if alreadyLearned {
						learner.LearnerLogF("Ignoring accepted msg for already learned instance: %d", msg.Id)
					} else {
						learner.UpdateAcceptedMsgQueue(msg.Id, msg.Ballot, msg.Value, msg.AcceptorId)
					}
				case "new_majority":
					learner.Majority = msg.Value.Val
					//learner.LearnerLogF(" set new majority: %d",learner.Majority)
				}
			}
		}
	}
}

func (learner *Learner) UpdateAcceptedMsgQueue(id, ballot int, value ProposalValue, acceptorId int ) {
	var instanceWithAccMsgs InstanceAccMsgs
	if len(learner.AccMsgQueue) == 0 {
		tmp := InstanceAccMsgs{
			Id: id,
			MsgList: make([]AccMsg, 0),
		}
		learner.AccMsgQueue = append(learner.AccMsgQueue, tmp)
	}
	var i int
	for ; i < len(learner.AccMsgQueue); i++ {
		if id == learner.AccMsgQueue[i].Id {
			 instanceWithAccMsgs = learner.AccMsgQueue[i]
		}
	}
	i = i -1
	wasRelevant, newMsgList := addToQueueForId(id, ballot, value, acceptorId, instanceWithAccMsgs.MsgList)
	newMsgListSize := len(newMsgList)
	if wasRelevant && newMsgListSize >= learner.Majority {
		
		if checkIfMajorityAccepted(value, learner.Majority, newMsgList) {
			learner.LearnerLogF("Majority of acceptors accepted a single value, we can learn it! id: %d, value: %d", id, value.Val)
			//learner.LearnerLogF("AccMsgQueue len: %d", len(learner.AccMsgQueue))
			learnMsg := &LearnerMsg {
				Action: "learn",
				Id: id,
				Value: value,
			}
			learner.Owner.LearnChan <- *learnMsg
			learner.AccMsgQueue = append(learner.AccMsgQueue[:i], learner.AccMsgQueue[i+1:]...)
			//learner.LearnerLogF("AccMsgQueue len: %d", len(learner.AccMsgQueue))
		} else {
			learner.LearnerLogF("Not enough acceptors accepted a single value! id: %d, value: %d, ballot: %d", id, value.Val, ballot)
			learner.AccMsgQueue[i].MsgList = newMsgList
		}
	} else {
		if wasRelevant {
			learner.LearnerLogF("Accept from learner %d enqueued, id: %d, value: %d, ballot %d ", acceptorId, id, value.Val, ballot)
			learner.AccMsgQueue[i].MsgList = newMsgList
		} else {
			learner.LearnerLogF("Ignoring accepted msg from %d, id: %d, value: %d, ballot %d ", acceptorId, id, value.Val, ballot)
		}
	}
}

func checkIfMajorityAccepted(value ProposalValue, majority int, newMsgList []AccMsg) bool {
	count := 0
	for i := 0; i < len(newMsgList); i++{
		if newMsgList[i].Value.Val == value.Val {
			count++
		}
	}
	return count >= majority
}

func addToQueueForId(id, ballot int, value ProposalValue, acceptorId int, msgList []AccMsg) (bool, []AccMsg) {
	for i := 0; i < len(msgList); i++ {
		if acceptorId == msgList[i].AcceptorId {
			if ballot <= msgList[i].Ballot {
				return false,msgList
			} else {
				msgList[i].Ballot = ballot
				msgList[i].Value = value
				return true, msgList
			}
		}
	}
	accMsg := AccMsg{
		ballot,
		value,
		acceptorId,
	}
	newMsgList := append(msgList, accMsg)
	return true, newMsgList
}


func (learner *Learner) LearnerLogF(format string, a ...interface{}) {
	learner.LearnerLog(fmt.Sprintf(format, a ...))
}

func (learner *Learner) LearnerLog(msg string) {
	learner.Owner.LogInfo("Learner: " + msg)
}