package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	proto "github.com/efferbach92-cmyk/Mandatory-Activity-5/grpc"
)

// En follower venter mellem 1,5 og 3 sekunder uden heartbeat, før den starter et valg.
// Ventetiden er tilfældig, så to followers ikke starter valg på samme tid.
// Den skal være klart længere end heartbeatInterval (500 ms).
const (
	electionTimeoutMin    = 1500 * time.Millisecond
	electionTimeoutSpread = 1500 * time.Millisecond
)

// isLeader læser rollen med låsen taget. Rollen kan nu ændre sig under et valg,
// så den må ikke læses uden lås.
func (s *NodeServer) isLeader() bool {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	return s.node.role == "leader"
}

// currentLeader returnerer id'et på den node, vi tror er leder lige nu.
func (s *NodeServer) currentLeader() int32 {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	return s.node.leaderID
}

// electionLoop kører i baggrunden på alle noder. Den er "stopuret":
// hvis der ikke er kommet et heartbeat inden for ventetiden, starter den et valg.
func (s *NodeServer) electionLoop() {
	n := s.node
	for {
		timeout := electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutSpread)))
		time.Sleep(timeout)

		n.mu.Lock()
		isLeader := n.role == "leader"
		// Vi har aldrig hørt fra en leder. Så er systemet stadig ved at starte op,
		// og vi starter ikke et valg, bare fordi node 1 ikke er tændt endnu.
		neverHeard := n.lastHeartbeat.IsZero()
		leaderAlive := time.Since(n.lastHeartbeat) < timeout
		n.mu.Unlock()

		if isLeader || neverHeard || leaderAlive {
			continue
		}
		s.startElection()
	}
}

// startElection gør noden til kandidat og beder de andre noder om deres stemme.
func (s *NodeServer) startElection() {
	n := s.node

	n.mu.Lock()
	n.term++ // nyt valg = ny term
	n.role = "candidate"
	n.votedFor = n.id            // stemmer på sig selv
	n.lastHeartbeat = time.Now() // genstarter stopuret, så vi ikke starter to valg oven i hinanden
	term := n.term
	req := &proto.VoteRequest{
		CandidateID:  n.id,
		Term:         term,
		LastLogIndex: int32(len(n.log)), // hvor mange bud vi har i loggen
	}
	n.mu.Unlock()

	log.Printf("[Node %d] event=ELECTION_START term=%d", n.id, term)

	votes := 1 // vores egen stemme
	for id, c := range s.nodeClients {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := c.RequestVote(ctx, req)
		cancel()
		if err != nil {
			// Noden svarer ikke. Det er forventet for den leder, der er crashet.
			log.Printf("[Node %d] event=VOTE_REQUEST_FAILED to=%d msg=%v", n.id, id, err)
			continue
		}
		if resp.Term > term {
			// En anden node er i en nyere term end os. Så er vores valg forældet.
			n.mu.Lock()
			if resp.Term > n.term {
				n.term = resp.Term
				n.votedFor = 0
			}
			n.role = "follower"
			n.mu.Unlock()
			return
		}
		if resp.VoteGranted {
			votes++
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Mens vi ventede på stemmer, kan en anden node være blevet leder.
	// Så har Heartbeat() eller RequestVote() allerede gjort os til follower.
	if n.role != "candidate" || n.term != term {
		return
	}
	// Flertal = mere end halvdelen af alle noder. Med 3 noder er det 2 stemmer.
	if votes <= len(nodeAddresses)/2 {
		log.Printf("[Node %d] event=ELECTION_LOST term=%d votes=%d", n.id, term, votes)
		n.role = "follower" // electionLoop prøver igen efter en ny tilfældig ventetid
		return
	}

	n.role = "leader"
	n.leaderID = n.id
	log.Printf("[Node %d] event=BECAME_LEADER term=%d votes=%d", n.id, term, votes)

	// Den gamle leder kan have godkendt bud, som vi har i loggen, men endnu ikke
	// har fået at vide er committed (det får en follower først at vide i næste heartbeat).
	// Vi sender derfor hele loggen til den anden follower og committer den,
	// så et bud, som en klient fik SUCCESS på, ikke forsvinder fra Result.
	if len(n.log) > int(n.commitIndex) {
		last := n.log[len(n.log)-1]
		if s.replicateBidToFollowers(context.Background(), last) {
			n.commitIndex = int32(len(n.log))
		}
	}
}

// RequestVote bliver kaldt af en kandidat, der vil have vores stemme.
func (s *NodeServer) RequestVote(ctx context.Context, req *proto.VoteRequest) (*proto.VoteResponse, error) {
	n := s.node
	n.mu.Lock()
	defer n.mu.Unlock()

	// 1. Kandidaten er i en ældre term end os. Dens valg er forældet.
	if req.Term < n.term {
		return &proto.VoteResponse{Term: n.term, VoteGranted: false}, nil
	}

	// 2. Kandidaten er i en nyere term. Vi rykker med op og har en ny stemme at give.
	if req.Term > n.term {
		n.term = req.Term
		n.votedFor = 0
		if n.role != "follower" {
			n.role = "follower"
			n.lastHeartbeat = time.Now()
		}
	}

	// 3. Man har kun én stemme pr. term.
	alreadyVoted := n.votedFor != 0 && n.votedFor != req.CandidateID

	// 4. Kandidatens log skal være mindst lige så lang som vores.
	//    Ellers kunne et bud, der allerede er godkendt, gå tabt.
	logOK := req.LastLogIndex >= int32(len(n.log))

	if alreadyVoted || !logOK {
		log.Printf("[Node %d] event=VOTE_DENIED candidate=%d term=%d", n.id, req.CandidateID, req.Term)
		return &proto.VoteResponse{Term: n.term, VoteGranted: false}, nil
	}

	n.votedFor = req.CandidateID
	n.lastHeartbeat = time.Now() // giver kandidaten tid til at blive leder, før vi selv starter et valg
	log.Printf("[Node %d] event=VOTE_GRANTED candidate=%d term=%d", n.id, req.CandidateID, req.Term)
	return &proto.VoteResponse{Term: n.term, VoteGranted: true}, nil
}
