package paxos

type PaxosNet struct {
	Agents map[string]*PaxosAgent
}

var paxosNet = PaxosNet{make(map[string]*PaxosAgent)}

func RegisterAgent(agent *PaxosAgent) bool {
	paxosNet.Agents[agent.Name] = agent
	majority := len(paxosNet.Agents) / 2 + 1
	majoritystruct := ProposalValue {
		Val:majority,
	}
	proposeMsg := &Proposal{
		Action: "new_majority",
		Value: majoritystruct,
	}
	learnMsg := &LearnerMsg{
		Action: "new_majority",
		Value: majoritystruct,
	}
	BroadcastProposeChan(proposeMsg)
	BroadcastLearnChan(learnMsg)
	return true
}

func BroadcastProposeChan(proposal *Proposal) {
	for _, agent := range paxosNet.Agents {
		agent.ProposeChan <- *proposal
	}
}

func BroadcastAcceptChan(acceptance *AcceptorMsg) {
	for _, agent := range paxosNet.Agents {
		agent.AcceptChan <- *acceptance
	}
}

func BroadcastLearnChan(learnMsg *LearnerMsg) {
	for _, agent := range paxosNet.Agents {
		agent.LearnChan <- *learnMsg
	}
}

func GetAgentByName(name string) (agent *PaxosAgent) {
	agent = paxosNet.Agents[name]
	return
}