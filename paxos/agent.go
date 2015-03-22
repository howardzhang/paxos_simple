package paxos

import (
	"time"
	)

//type Msg struct {
//	Cmd string
//	Content string
//}

//type PaxosAgent interface {
//	RecieveProposal(msg *Msg)
//	RecieveAccpetance(msg *Msg)
//	RecieveLearnmsg(msg *Msg)
//}

var MAX_NUM_OF_PROPOSERS = 10

type PaxosAgent struct {
	Id int
	Name string
	ProposeChan chan Proposal
	AcceptChan  chan AcceptorMsg
	LearnChan   chan LearnerMsg
	Log chan string
	ProposalId int
	Majority int
	InstRecords []InstRecord
	Phase1Records []Phase1Record
}

func (agent *PaxosAgent) AgentStart() {
	proposer := &Proposer{
		Owner:agent,
	}
	acceptor := &Acceptor{
		Owner:agent,
	}
	learner := &Learner{
		Owner:agent,
	}
	go proposer.ProposerLoop()
	go acceptor.AcceptorLoop()
	go learner.LearnerLoop()
	agentloop:
	for {
		select {
			case <- time.After(time.Second * 20): {
			    agent.LogInfo(" time out!")
				break agentloop
			}
		}
	}
	close(agent.ProposeChan)
	close(agent.AcceptChan)
	close(agent.LearnChan)
}

func (agent *PaxosAgent) Propose(val int) {
	valstruct := ProposalValue {
		Val:val,
	}
	msg := &Proposal{
		Action: "start_new_proposal",
		Value: valstruct,
	}
	agent.ProposeChan <- *msg
}

func (agent *PaxosAgent) LogInfo(msg string) {
	agent.Log <- "[" + agent.Name + "::INFO]: " + msg
}