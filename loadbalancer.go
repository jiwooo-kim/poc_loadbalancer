package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Request struct represents the structure of incoming requests
type Request struct {
	BPM int `json:"bpm"`
}

// NodeLimits struct represents the limits of a node
type NodeLimits struct {
	NodeID    string `bson:"node_id"`
	RPMLimit  int    `bson:"rpm_limit"`
	BPMLimit  int    `bson:"bpm_limit"`
	Timestamp time.Time
}

// RequestInfo struct represents information about a request
type RequestInfo struct {
	NodeID      string `bson:"node_id"`
	Timestamp   time.Time
	BPM         int
	RequestsCnt int
	TotalBPM    int
}

// MongoDB connection
var (
	client          *mongo.Client
	database        *mongo.Database
	nodeCollection  *mongo.Collection
	requestsCollection *mongo.Collection
)

func init() {
	// MongoDB connection
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017/")
	var err error
	client, err = mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	database = client.Database("rate_limit_db")
	nodeCollection = database.Collection("node_limits")
	requestsCollection = database.Collection("requests")
}

// LoadBalancer struct represents the load balancer
type LoadBalancer struct {
	NodeLimits map[string]NodeLimits
}

func (lb *LoadBalancer) getAvailableNodes() []string {
	currentTime := time.Now().Add(-time.Minute)

	// Aggregate query to get available nodes
	availableNodesQuery, err := requestsCollection.Aggregate(context.Background(), bson.D{
		{"$match", bson.D{
			{"timestamp", bson.D{{"$gt", currentTime}}},
		}},
		{"$group", bson.D{
			{"_id", "$node_id"},
			{"requests_count", bson.D{{"$sum", 1}}},
			{"total_bpm", bson.D{{"$sum", "$bpm"}}},
		}},
	})
	if err != nil {
		log.Fatal(err)
	}

	availableNodes := []string{}
	for availableNodesQuery.Next(context.Background()) {
		var nodeInfo RequestInfo
		err := availableNodesQuery.Decode(&nodeInfo)
		if err != nil {
			log.Fatal(err)
		}

		if nodeInfo.RequestsCnt < lb.NodeLimits[nodeInfo.NodeID].RPMLimit && nodeInfo.TotalBPM < lb.NodeLimits[nodeInfo.NodeID].BPMLimit {
			availableNodes = append(availableNodes, nodeInfo.NodeID)
		}
	}
	return availableNodes
}

func (lb *LoadBalancer) selectNode() string {
	availableNodes := lb.getAvailableNodes()
	if len(availableNodes) > 0 {
		return availableNodes[rand.Intn(len(availableNodes))]
	}
	return ""
}

func (lb *LoadBalancer) sendRequestToNode(nodeID string, request *Request) {
	// Simulate sending request
	fmt.Printf("Forwarding request to node %s: %+v\n", nodeID, request)
	// In a real system, you would forward the request to the actual node

	// Update BPM in the database
	_, err := requestsCollection.InsertOne(context.Background(), bson.D{
		{"timestamp", time.Now()},
		{"node_id", nodeID},
		{"bpm", request.BPM},
	})
	if err != nil {
		log.Fatal(err)
	}
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	var request Request
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	selectedNode := loadBalancer.selectNode()
	if selectedNode != "" {
		loadBalancer.sendRequestToNode(selectedNode, &request)
		response := map[string]string{"status": "success", "message": fmt.Sprintf("Request forwarded to node %s", selectedNode)}
		json.NewEncoder(w).Encode(response)
	} else {
		http.Error(w, "All nodes are currently at rate limit. Retry later.", http.StatusTooManyRequests)
	}
}

func main() {
	loadBalancer := LoadBalancer{}

	// Initialize router
	router := mux.NewRouter()

	// Define routes
	router.HandleFunc("/request", handleRequest).Methods("POST")

	// Start server
	fmt.Println("Server listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}

