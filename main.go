package main

import (
	"fmt"
	"time"
	"os"
	"bufio"
	"strings"
	"strconv"
	"github.com/hwcheung/paxos_simple/paxos"
	)

func main() {
	fmt.Println("Simulating Paxos Algorithm!")
	log := make(chan string, 20)
	agentA := &paxos.PaxosAgent{ Id:0, Name:"agentA", 
		ProposeChan:make(chan paxos.Proposal,10), 
		AcceptChan:make(chan paxos.AcceptorMsg), 
		LearnChan:make(chan paxos.LearnerMsg, 10),
		}
	agentA.Log = log
	agentB := &paxos.PaxosAgent{ Id:1, Name:"agentB", 
		ProposeChan:make(chan paxos.Proposal,10), 
		AcceptChan:make(chan paxos.AcceptorMsg), 
		LearnChan:make(chan paxos.LearnerMsg, 10),
		}
	agentB.Log = log
	agentC := &paxos.PaxosAgent{ Id:2, Name:"agentC", 
		ProposeChan:make(chan paxos.Proposal,10), 
		AcceptChan:make(chan paxos.AcceptorMsg), 
		LearnChan:make(chan paxos.LearnerMsg, 10),
		}
	agentC.Log = log
	
	go agentA.AgentStart()
	go agentB.AgentStart()
	go agentC.AgentStart()
	
	paxos.RegisterAgent(agentA)
	paxos.RegisterAgent(agentB)
	paxos.RegisterAgent(agentC)
	
	cmd := make(chan string, 10)
	go func(log chan string, cmd chan string) {
		reader := bufio.NewReader(os.Stdin)
		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				log <- "Stdin closed"
				return
			}
			cmd <- s
		}
	}(log, cmd)
	
logloop:	
	for {
		select {
			case logMsg := <- log: {
				fmt.Println(logMsg)
			}
			case cmdMsg := <- cmd:{
				fmt.Println("Std Input: " + cmdMsg)
				agentName := strings.Split(cmdMsg, ",")[0]
				valS := strings.TrimSpace(strings.Split(cmdMsg, ",")[1])
				agent := paxos.GetAgentByName(agentName)
				val, _ := strconv.Atoi(valS)
				log <- agentName + " is proposing value: " + strconv.Itoa(val)
				agent.Propose(val)
			}
			case <- time.After(time.Second * 15):{
				break logloop
			}
		}
		
	}
	
}