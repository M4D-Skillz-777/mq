package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mqpb "github.com/adwaiy/mq/proto"
)

var (
	address   string
	conn      *grpc.ClientConn
	client    mqpb.MessageQueueClient
	ctx       context.Context
	cancelCtx context.CancelFunc
)

func init() {
	ctx, cancelCtx = context.WithCancel(context.Background())
}

func connect() error {
	if conn == nil {
		var err error
		conn, err = grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to %s: %v", address, err)
		}
		client = mqpb.NewMessageQueueClient(conn)
	}
	return nil
}

func disconnect() {
	if conn != nil {
		conn.Close()
		conn = nil
		client = nil
	}
}

var rootCmd = &cobra.Command{
	Use:   "dmq",
	Short: "DMQ CLI - Distributed Message Queue CLI tool",
	Long: `DMQ CLI - An interactive CLI for Distributed Message Queue
Like redis-cli or kafka-cli, but for DMQ.

Examples:
  dmq topic list
  dmq topic create my-topic --partitions 3
  dmq produce my-topic --key "key1" --value "hello"
  dmq consume my-topic --partition 0`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return connect()
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		disconnect()
		return nil
	},
}

func main() {
	rootCmd.PersistentFlags().StringVarP(&address, "address", "a", ":9001", "Broker address")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output")

	rootCmd.AddCommand(topicCmd)
	rootCmd.AddCommand(produceCmd)
	rootCmd.AddCommand(consumeCmd)
	rootCmd.AddCommand(groupCmd)
	rootCmd.AddCommand(clusterCmd)
	rootCmd.AddCommand(shellCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

var verbose bool

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Topic management commands",
}

var topicListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all topics",
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := client.ListTopics(ctx, &mqpb.ListTopicsRequest{})
		if err != nil {
			return fmt.Errorf("failed to list topics: %v", err)
		}
		if len(resp.Topics) == 0 {
			fmt.Println("No topics found")
			return nil
		}
		fmt.Println("Topics:")
		for _, t := range resp.Topics {
			fmt.Printf("  - %s\n", t)
		}
		return nil
	},
}

var topicCreateCmd = &cobra.Command{
	Use:   "create <topic>",
	Short: "Create a new topic",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		partitions, _ := cmd.Flags().GetInt32("partitions")
		replicationFactor, _ := cmd.Flags().GetInt32("replication-factor")

		resp, err := client.CreateTopic(ctx, &mqpb.CreateTopicRequest{
			Topic:             args[0],
			Partitions:        partitions,
			ReplicationFactor: replicationFactor,
		})
		if err != nil {
			return fmt.Errorf("failed to create topic: %v", err)
		}
		if resp.Success {
			fmt.Printf("Topic '%s' created successfully with %d partitions\n", args[0], partitions)
		} else {
			fmt.Printf("Failed to create topic: %s\n", resp.Error)
		}
		return nil
	},
}

var topicDescribeCmd = &cobra.Command{
	Use:   "describe <topic>",
	Short: "Describe a topic",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := client.DescribeTopic(ctx, &mqpb.DescribeTopicRequest{Topic: args[0]})
		if err != nil {
			return fmt.Errorf("failed to describe topic: %v", err)
		}
		fmt.Printf("Topic: %s\n", resp.Topic)
		fmt.Printf("Partitions: %d\n", resp.Partitions)
		if len(resp.PartitionInfo) > 0 {
			fmt.Println("Partition Info:")
			for _, p := range resp.PartitionInfo {
				fmt.Printf("  Partition %d:\n", p.PartitionId)
				fmt.Printf("    High Watermark: %d\n", p.HighWaterMark)
				fmt.Printf("    Log End Offset: %d\n", p.LogEndOffset)
				fmt.Printf("    Leader: %d\n", p.Leader)
				fmt.Printf("    Replicas: %v\n", p.Replicas)
				fmt.Printf("    ISR: %v\n", p.Isr)
			}
		}
		return nil
	},
}

var topicDeleteCmd = &cobra.Command{
	Use:   "delete <topic>",
	Short: "Delete a topic",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := client.DeleteTopic(ctx, &mqpb.DeleteTopicRequest{Topic: args[0]})
		if err != nil {
			return fmt.Errorf("failed to delete topic: %v", err)
		}
		if resp.Success {
			fmt.Printf("Topic '%s' deleted successfully\n", args[0])
		} else {
			fmt.Printf("Failed to delete topic: %s\n", resp.Error)
		}
		return nil
	},
}

func init() {
	topicCmd.AddCommand(topicListCmd)
	topicCmd.AddCommand(topicCreateCmd)
	topicCmd.AddCommand(topicDescribeCmd)
	topicCmd.AddCommand(topicDeleteCmd)

	topicCreateCmd.Flags().Int32P("partitions", "p", 1, "Number of partitions")
	topicCreateCmd.Flags().Int32P("replication-factor", "r", 1, "Replication factor")
}

var produceCmd = &cobra.Command{
	Use:   "produce <topic>",
	Short: "Produce messages to a topic",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key, _ := cmd.Flags().GetString("key")
		value, _ := cmd.Flags().GetString("value")
		valueFile, _ := cmd.Flags().GetString("value-file")
		partition, _ := cmd.Flags().GetInt32("partition")
		acks, _ := cmd.Flags().GetInt32("acks")

		if valueFile != "" {
			data, err := os.ReadFile(valueFile)
			if err != nil {
				return fmt.Errorf("failed to read value file: %v", err)
			}
			value = string(data)
		}

		req := &mqpb.ProduceRequest{
			Topic:     args[0],
			Partition: partition,
			Acks:      acks,
		}

		if key != "" {
			req.Key = []byte(key)
		}
		if value != "" {
			req.Value = []byte(value)
		}

		resp, err := client.Produce(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to produce: %v", err)
		}

		fmt.Printf("Message produced successfully\n")
		fmt.Printf("  Topic: %s\n", args[0])
		fmt.Printf("  Partition: %d\n", resp.Partition)
		fmt.Printf("  Offset: %d\n", resp.Offset)
		return nil
	},
}

var produceStreamCmd = &cobra.Command{
	Use:   "produce-stream <topic>",
	Short: "Stream messages to a topic (interactive mode)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		partition, _ := cmd.Flags().GetInt32("partition")

		fmt.Printf("Streaming mode for topic '%s'. Type messages and press Enter to produce.\n", args[0])
		fmt.Printf("Special commands: :quit to exit, :help for help\n\n")

		offset := int64(0)
		for {
			fmt.Printf("[%s:%d] > ", args[0], partition)
			var input string
			fmt.Scanln(&input)

			if input == ":quit" || input == ":q" {
				fmt.Println("Exiting stream mode")
				break
			}

			if input == ":help" || input == ":h" {
				fmt.Println("Commands:")
				fmt.Println("  :quit, :q   - Exit streaming mode")
				fmt.Println("  :help, :h   - Show this help")
				fmt.Println("  :part N     - Switch to partition N")
				continue
			}

			if strings.HasPrefix(input, ":part ") {
				p, err := strconv.Atoi(strings.TrimSpace(input[6:]))
				if err == nil {
					partition = int32(p)
					fmt.Printf("Switched to partition %d\n", partition)
				}
				continue
			}

			_, err := client.Produce(ctx, &mqpb.ProduceRequest{
				Topic:     args[0],
				Partition: partition,
				Value:     []byte(input),
			})
			if err != nil {
				fmt.Printf("Error producing: %v\n", err)
				continue
			}
			fmt.Printf("  -> offset: %d\n", offset)
			offset++
		}
		return nil
	},
}

func init() {
	produceCmd.Flags().StringP("key", "k", "", "Message key")
	produceCmd.Flags().StringP("value", "v", "", "Message value")
	produceCmd.Flags().StringP("value-file", "f", "", "Read value from file")
	produceCmd.Flags().Int32P("partition", "p", -1, "Partition (-1 for auto)")
	produceCmd.Flags().Int32P("acks", "a", 1, "Number of acknowledgments")

	produceStreamCmd.Flags().Int32P("partition", "p", 0, "Partition")

	produceCmd.AddCommand(produceStreamCmd)
}

var consumeCmd = &cobra.Command{
	Use:   "consume <topic>",
	Short: "Consume messages from a topic",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		partition, _ := cmd.Flags().GetInt32("partition")
		offset, _ := cmd.Flags().GetInt64("offset")
		maxBytes, _ := cmd.Flags().GetInt32("max-bytes")
		count, _ := cmd.Flags().GetInt("count")

		req := &mqpb.ConsumeRequest{
			Topic:     args[0],
			Partition: partition,
			Offset:    offset,
			MaxBytes:  maxBytes,
		}

		resp, err := client.Consume(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to consume: %v", err)
		}

		fmt.Printf("High Watermark: %d\n", resp.HighWatermark)
		fmt.Printf("Messages received: %d\n\n", len(resp.Messages))

		messages := resp.Messages
		if count > 0 && len(messages) > count {
			messages = messages[:count]
		}

		for i, msg := range messages {
			fmt.Printf("[%d] Offset: %d, Partition: %d\n", i, msg.Offset, msg.Partition)
			if len(msg.Key) > 0 {
				fmt.Printf("    Key: %s\n", string(msg.Key))
			}
			fmt.Printf("    Value: %s\n", string(msg.Value))
			if len(msg.Headers) > 0 {
				fmt.Printf("    Headers: %v\n", msg.Headers)
			}
			fmt.Println()
		}
		return nil
	},
}

var subscribeCmd = &cobra.Command{
	Use:   "subscribe <topic>",
	Short: "Subscribe to a topic (streaming)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		group, _ := cmd.Flags().GetString("group")
		partition, _ := cmd.Flags().GetInt32("partition")
		startOffset, _ := cmd.Flags().GetInt64("offset")

		if group == "" {
			return fmt.Errorf("group is required for subscribe")
		}

		req := &mqpb.SubscribeRequest{
			GroupId:     group,
			Topic:       args[0],
			Partition:   partition,
			StartOffset: startOffset,
		}

		stream, err := client.Subscribe(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to subscribe: %v", err)
		}

		fmt.Printf("Subscribed to topic '%s' in group '%s'\n", args[0], group)
		fmt.Printf("Press Ctrl+C to exit\n\n")

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		msgCount := 0
		for {
			select {
			case <-sigChan:
				fmt.Println("\nExiting...")
				return nil
			default:
				msg, err := stream.Recv()
				if err == io.EOF {
					fmt.Println("Stream ended")
					return nil
				}
				if err != nil {
					return fmt.Errorf("stream error: %v", err)
				}

				msgCount++
				fmt.Printf("[%d] Offset: %d, Partition: %d\n", msgCount, msg.Offset, msg.Partition)
				if len(msg.Key) > 0 {
					fmt.Printf("    Key: %s\n", string(msg.Key))
				}
				fmt.Printf("    Value: %s\n", string(msg.Value))
				fmt.Println()
			}
		}
	},
}

func init() {
	consumeCmd.Flags().Int32P("partition", "p", 0, "Partition")
	consumeCmd.Flags().Int64P("offset", "o", 0, "Starting offset")
	consumeCmd.Flags().Int32P("max-bytes", "b", 1048576, "Max bytes to fetch")
	consumeCmd.Flags().IntP("count", "c", 0, "Max messages to receive (0 = all)")

	subscribeCmd.Flags().StringP("group", "g", "", "Consumer group")
	subscribeCmd.Flags().Int32P("partition", "p", 0, "Partition")
	subscribeCmd.Flags().Int64P("offset", "o", 0, "Starting offset")

	consumeCmd.AddCommand(subscribeCmd)
}

var groupCmd = &cobra.Command{
	Use:   "group",
	Short: "Consumer group management",
}

var groupJoinCmd = &cobra.Command{
	Use:   "join <group-id> <member-id> [topics...]",
	Short: "Join a consumer group",
	Args:  cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		groupId := args[0]
		memberId := args[1]
		topics := args[2:]

		if len(topics) == 0 {
			return fmt.Errorf("at least one topic is required")
		}

		resp, err := client.JoinGroup(ctx, &mqpb.JoinGroupRequest{
			GroupId:  groupId,
			MemberId: memberId,
			Topics:   topics,
		})
		if err != nil {
			return fmt.Errorf("failed to join group: %v", err)
		}

		fmt.Printf("Successfully joined group '%s'\n", groupId)
		fmt.Printf("  Member ID: %s\n", resp.MemberId)
		fmt.Printf("  Generation ID: %d\n", resp.GenerationId)
		fmt.Printf("  Leader ID: %s\n", resp.LeaderId)
		fmt.Printf("  Members: %v\n", resp.Members)
		return nil
	},
}

var groupLeaveCmd = &cobra.Command{
	Use:   "leave <group-id> <member-id>",
	Short: "Leave a consumer group",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := client.LeaveGroup(ctx, &mqpb.LeaveGroupRequest{
			GroupId:  args[0],
			MemberId: args[1],
		})
		if err != nil {
			return fmt.Errorf("failed to leave group: %v", err)
		}

		if resp.Success {
			fmt.Printf("Successfully left group '%s'\n", args[0])
		} else {
			fmt.Printf("Failed to leave group\n")
		}
		return nil
	},
}

var groupCommitCmd = &cobra.Command{
	Use:   "commit <group-id> <topic> <partition> <offset>",
	Short: "Commit offset for a consumer group",
	Args:  cobra.ExactArgs(4),
	RunE: func(cmd *cobra.Command, args []string) error {
		partition, err := strconv.Atoi(args[2])
		if err != nil {
			return fmt.Errorf("invalid partition: %v", err)
		}
		offset, err := strconv.Atoi(args[3])
		if err != nil {
			return fmt.Errorf("invalid offset: %v", err)
		}

		resp, err := client.CommitOffset(ctx, &mqpb.CommitOffsetRequest{
			GroupId:   args[0],
			Topic:     args[1],
			Partition: int32(partition),
			Offset:    int64(offset),
		})
		if err != nil {
			return fmt.Errorf("failed to commit offset: %v", err)
		}

		if resp.Success {
			fmt.Printf("Successfully committed offset %d for %s:%d\n", offset, args[1], partition)
		} else {
			fmt.Printf("Failed to commit offset\n")
		}
		return nil
	},
}

func init() {
	groupCmd.AddCommand(groupJoinCmd)
	groupCmd.AddCommand(groupLeaveCmd)
	groupCmd.AddCommand(groupCommitCmd)
}

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster management commands",
}

var clusterInfoCmd = &cobra.Command{
	Use:   "info",
	Short: "Show cluster information",
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := client.DescribeCluster(ctx, &mqpb.DescribeClusterRequest{})
		if err != nil {
			return fmt.Errorf("failed to describe cluster: %v", err)
		}

		fmt.Printf("Cluster ID: %s\n", resp.ClusterId)
		fmt.Printf("Broker ID: %d\n", resp.BrokerId)
		fmt.Printf("Brokers: %d\n", len(resp.Brokers))
		for _, b := range resp.Brokers {
			fmt.Printf("  - Broker %d: %s:%d\n", b.Id, b.Host, b.Port)
		}
		return nil
	},
}

func init() {
	clusterCmd.AddCommand(clusterInfoCmd)
}

var shellCmd = &cobra.Command{
	Use:   "shell",
	Short: "Start interactive shell",
	Long: `Start an interactive shell for DMQ.
Type commands directly without the 'dmq' prefix.
Type 'help' for available commands, 'exit' or 'quit' to exit.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runShell()
	},
}

func runShell() error {
	if err := connect(); err != nil {
		return err
	}

	fmt.Println("DMQ Interactive Shell")
	fmt.Println("Type 'help' for commands, 'exit' or 'quit' to exit")
	fmt.Println()

	for {
		fmt.Print("dmq> ")
		var input string
		fmt.Scanln(&input)

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		if input == "exit" || input == "quit" || input == ":q" {
			fmt.Println("Goodbye!")
			break
		}

		if input == "help" || input == "?" {
			printHelp()
			continue
		}

		handleShellCommand(input)
	}

	disconnect()
	return nil
}

func printHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  topic list                           - List all topics")
	fmt.Println("  topic create <name> [partitions]    - Create a topic")
	fmt.Println("  topic describe <name>               - Describe a topic")
	fmt.Println("  topic delete <name>                 - Delete a topic")
	fmt.Println("  produce <topic> [-k key] [-v value] - Produce a message")
	fmt.Println("  produce-stream <topic>               - Stream messages (interactive)")
	fmt.Println("  consume <topic> [-p partition]      - Consume messages")
	fmt.Println("  subscribe <topic> -g <group>         - Subscribe to topic")
	fmt.Println("  group join <group> <member> <topics>  - Join a group")
	fmt.Println("  group leave <group> <member>         - Leave a group")
	fmt.Println("  cluster info                        - Show cluster info")
	fmt.Println("  help                                - Show this help")
	fmt.Println("  exit, quit                          - Exit shell")
}

func handleShellCommand(input string) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return
	}

	command := parts[0]
	args := parts[1:]

	switch command {
	case "topic":
		handleTopicCommand(args)
	case "produce":
		handleProduceCommand(args)
	case "consume":
		handleConsumeCommand(args)
	case "subscribe":
		handleSubscribeCommand(args)
	case "group":
		handleGroupCommand(args)
	case "cluster":
		handleClusterCommand(args)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		fmt.Println("Type 'help' for available commands")
	}
}

func handleTopicCommand(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: topic <list|create|describe|delete> [options]")
		return
	}

	switch args[0] {
	case "list":
		resp, err := client.ListTopics(ctx, &mqpb.ListTopicsRequest{})
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		if len(resp.Topics) == 0 {
			fmt.Println("No topics found")
			return
		}
		fmt.Println("Topics:")
		for _, t := range resp.Topics {
			fmt.Printf("  - %s\n", t)
		}

	case "create":
		if len(args) < 2 {
			fmt.Println("Usage: topic create <name> [partitions]")
			return
		}
		partitions := int32(1)
		if len(args) >= 3 {
			p, _ := strconv.Atoi(args[2])
			partitions = int32(p)
		}
		resp, err := client.CreateTopic(ctx, &mqpb.CreateTopicRequest{
			Topic:      args[1],
			Partitions: partitions,
		})
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		if resp.Success {
			fmt.Printf("Topic '%s' created with %d partitions\n", args[1], partitions)
		} else {
			fmt.Printf("Error: %s\n", resp.Error)
		}

	case "describe":
		if len(args) < 2 {
			fmt.Println("Usage: topic describe <name>")
			return
		}
		resp, err := client.DescribeTopic(ctx, &mqpb.DescribeTopicRequest{Topic: args[1]})
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("Topic: %s, Partitions: %d\n", resp.Topic, resp.Partitions)

	case "delete":
		if len(args) < 2 {
			fmt.Println("Usage: topic delete <name>")
			return
		}
		resp, err := client.DeleteTopic(ctx, &mqpb.DeleteTopicRequest{Topic: args[1]})
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		if resp.Success {
			fmt.Printf("Topic '%s' deleted\n", args[1])
		} else {
			fmt.Printf("Error: %s\n", resp.Error)
		}

	default:
		fmt.Println("Unknown topic command. Use: list, create, describe, delete")
	}
}

func handleProduceCommand(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: produce <topic> [-k key] [-v value]")
		return
	}

	topic := args[0]
	var key, value string
	var partition int32 = -1

	for i := 1; i < len(args); i++ {
		if args[i] == "-k" && i+1 < len(args) {
			key = args[i+1]
			i++
		} else if args[i] == "-v" && i+1 < len(args) {
			value = args[i+1]
			i++
		} else if args[i] == "-p" && i+1 < len(args) {
			p, _ := strconv.Atoi(args[i+1])
			partition = int32(p)
			i++
		}
	}

	if value == "" {
		fmt.Print("Enter message value: ")
		fmt.Scanln(&value)
	}

	req := &mqpb.ProduceRequest{
		Topic:     topic,
		Partition: partition,
	}
	if key != "" {
		req.Key = []byte(key)
	}
	if value != "" {
		req.Value = []byte(value)
	}

	resp, err := client.Produce(ctx, req)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Produced to %s:%d at offset %d\n", topic, resp.Partition, resp.Offset)
}

func handleConsumeCommand(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: consume <topic> [-p partition] [-o offset]")
		return
	}

	topic := args[0]
	partition := int32(0)
	offset := int64(0)

	for i := 1; i < len(args); i++ {
		if args[i] == "-p" && i+1 < len(args) {
			p, _ := strconv.Atoi(args[i+1])
			partition = int32(p)
			i++
		} else if args[i] == "-o" && i+1 < len(args) {
			o, _ := strconv.Atoi(args[i+1])
			offset = int64(o)
			i++
		}
	}

	resp, err := client.Consume(ctx, &mqpb.ConsumeRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Messages (%d):\n", len(resp.Messages))
	for _, msg := range resp.Messages {
		keyStr := ""
		if len(msg.Key) > 0 {
			keyStr = fmt.Sprintf(" key=%s", string(msg.Key))
		}
		fmt.Printf("  [%d]%s value=%s\n", msg.Offset, keyStr, string(msg.Value))
	}
}

func handleSubscribeCommand(args []string) {
	var topic, group string
	var partition int32 = 0

	for i := 0; i < len(args); i++ {
		if args[i] == "-g" && i+1 < len(args) {
			group = args[i+1]
			i++
		} else if args[i] == "-p" && i+1 < len(args) {
			p, _ := strconv.Atoi(args[i+1])
			partition = int32(p)
			i++
		} else if topic == "" {
			topic = args[i]
		}
	}

	if topic == "" || group == "" {
		fmt.Println("Usage: subscribe <topic> -g <group> [-p partition]")
		return
	}

	stream, err := client.Subscribe(ctx, &mqpb.SubscribeRequest{
		GroupId:   group,
		Topic:     topic,
		Partition: partition,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Subscribed. Waiting for messages (Ctrl+C to exit)...\n")
	msgCount := 0
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			break
		}
		msgCount++
		keyStr := ""
		if len(msg.Key) > 0 {
			keyStr = fmt.Sprintf(" key=%s", string(msg.Key))
		}
		fmt.Printf("[%d] offset=%d%s value=%s\n", msgCount, msg.Offset, keyStr, string(msg.Value))
	}
}

func handleGroupCommand(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: group <join|leave|info> [options]")
		return
	}

	switch args[0] {
	case "join":
		if len(args) < 4 {
			fmt.Println("Usage: group join <group-id> <member-id> <topics...>")
			return
		}
		groupId := args[1]
		memberId := args[2]
		topics := args[3:]

		resp, err := client.JoinGroup(ctx, &mqpb.JoinGroupRequest{
			GroupId:  groupId,
			MemberId: memberId,
			Topics:   topics,
		})
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("Joined group %s as %s (generation %d)\n", groupId, resp.MemberId, resp.GenerationId)

	case "leave":
		if len(args) < 3 {
			fmt.Println("Usage: group leave <group-id> <member-id>")
			return
		}
		resp, err := client.LeaveGroup(ctx, &mqpb.LeaveGroupRequest{
			GroupId:  args[1],
			MemberId: args[2],
		})
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		if resp.Success {
			fmt.Printf("Left group %s\n", args[1])
		}

	default:
		fmt.Println("Unknown group command. Use: join, leave")
	}
}

func handleClusterCommand(args []string) {
	if len(args) == 0 || args[0] == "info" {
		resp, err := client.DescribeCluster(ctx, &mqpb.DescribeClusterRequest{})
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("Cluster ID: %s, Broker ID: %d\n", resp.ClusterId, resp.BrokerId)
		fmt.Printf("Brokers: %d\n", len(resp.Brokers))
		for _, b := range resp.Brokers {
			fmt.Printf("  - Broker %d: %s:%d\n", b.Id, b.Host, b.Port)
		}
	}
}
