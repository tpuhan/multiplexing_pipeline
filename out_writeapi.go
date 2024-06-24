package main

import (
	"C"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/fluent/fluent-bit-go/output"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type StreamConfig struct {
	projectID     string
	datasetID     string
	tableID       string
	md            protoreflect.MessageDescriptor
	managedStream *managedwriter.ManagedStream
	client        *managedwriter.Client
}

var (
	// projectID     string
	// datasetID     string
	// tableID       string
	// md            protoreflect.MessageDescriptor
	// managedStream *managedwriter.ManagedStream
	client    *managedwriter.Client
	ms_ctx    context.Context
	configMap = make(map[string]StreamConfig)
)

// This function handles getting data on the schema of the table data is being written to.
func getDescriptors(ms_ctx context.Context, managed_writer_client *managedwriter.Client, project string, dataset string, table string) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto, error) {
	curr_stream := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/_default", project, dataset, table)
	req := storagepb.GetWriteStreamRequest{
		Name: curr_stream,
		View: storagepb.WriteStreamView_FULL,
	}

	table_data, err := managed_writer_client.GetWriteStream(ms_ctx, &req)
	if err != nil {
		return nil, nil, fmt.Errorf("getWriteStream command failed: %v", err)
	}

	table_schema := table_data.GetTableSchema()
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(table_schema, "root")
	if err != nil {
		return nil, nil, fmt.Errorf("adapt.StorageSchemaToDescriptor: %v", err)
	}

	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, nil, fmt.Errorf("adapted descriptor is not a message descriptor")
	}

	dp, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, nil, fmt.Errorf("NormalizeDescriptor: %v", err)
	}

	return messageDescriptor, dp, nil
}

// This function handles the data transformation from JSON to binary for a single json row.
func jsonToBinary(messageDescriptor protoreflect.MessageDescriptor, jsonRow map[string]interface{}) ([]byte, error) {
	row, err := json.Marshal(jsonRow)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json map: %w", err)
	}

	message := dynamicpb.NewMessage(messageDescriptor)

	err = protojson.Unmarshal(row, message)
	if err != nil {
		return nil, fmt.Errorf("failed to Unmarshal json message: %w", err)
	}

	b, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal proto bytes: %w", err)
	}

	return b, nil
}

// Function to transform Fluent Bit record to a JSON map
func parseMap(mapInterface map[interface{}]interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	for k, v := range mapInterface {
		switch t := v.(type) {
		case []byte:
			m[k.(string)] = string(t)
		case map[interface{}]interface{}:
			m[k.(string)] = parseMap(t)
		default:
			m[k.(string)] = v
		}
	}
	return m
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	log.Printf("[multiinstance] Register called")
	return output.FLBPluginRegister(def, "writeapi", "Testing multiple instances.")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	id := output.FLBPluginConfigKey(plugin, "OutputID")
	log.Printf("[multiinstance] id = %q", id)
	// Set the context to point to any Go variable
	output.FLBPluginSetContext(plugin, id)

	//create context
	ms_ctx = context.Background()

	//set projectID, datasetID, and tableID from config file params
	projectID := output.FLBPluginConfigKey(plugin, "ProjectID")
	datasetID := output.FLBPluginConfigKey(plugin, "DatasetID")
	tableID := output.FLBPluginConfigKey(plugin, "TableID")

	tableReference := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID)

	//create new client
	client, err := managedwriter.NewClient(ms_ctx, projectID)
	if err != nil {
		log.Fatal(err)
		return output.FLB_ERROR
	}

	//use getDescriptors to get the message descriptor, and descriptor proto
	md, descriptor, err := getDescriptors(ms_ctx, client, projectID, datasetID, tableID)
	if err != nil {
		log.Fatalf("Failed to get descriptors: %v", err)
		return output.FLB_ERROR
	}

	managedStream, err := client.NewManagedStream(ms_ctx,
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithDestinationTable(tableReference),
		managedwriter.WithSchemaDescriptor(descriptor),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		log.Fatalf("Failed to create managed stream: %v", err)
		return output.FLB_ERROR
	}

	config := StreamConfig{
		projectID:     projectID,
		datasetID:     datasetID,
		tableID:       tableID,
		md:            md,
		managedStream: managedStream,
		client:        client,
	}

	configMap[id] = config

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	log.Print("[multiinstance] Flush called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	// Type assert context back into the original type for the Go variable
	id := output.FLBPluginGetContext(ctx).(string)
	log.Printf("[multiinstance] Flush called for id: %s", id)

	config, ok := configMap[id]
	if !ok {
		log.Printf("Skipping flush because config is not found for tag: %s.", id)
		return output.FLB_OK
	}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))
	var binaryData [][]byte

	// Iterate Records
	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		row := parseMap(record)

		buf, err := jsonToBinary(config.md, row)
		if err != nil {
			log.Fatalf("Failed to convert from JSON to binary: %v", err)
			return output.FLB_ERROR
		}
		binaryData = append(binaryData, buf)
	}

	// Append rows
	stream, err := config.managedStream.AppendRows(ms_ctx, binaryData)
	if err != nil {
		log.Fatalf("Failed to append rows: %v", err)
		return output.FLB_ERROR
	}

	// Check result
	_, err = stream.GetResult(ms_ctx)
	if err != nil {
		log.Fatalf("Append returned error: %v", err)
		return output.FLB_ERROR
	}

	log.Printf("Done!")

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	log.Print("[multiinstance] Exit called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	// Type assert context back into the original type for the Go variable
	id := output.FLBPluginGetContext(ctx).(string)
	log.Printf("[multiinstance] Exit called for id: %s", id)

	config, ok := configMap[id]
	if !ok {
		log.Printf("Skipping exit because config is not found for tag: %s.", id)
		return output.FLB_OK
	}

	if config.managedStream != nil {
		if err := config.managedStream.Close(); err != nil {
			log.Printf("Couldn't close managed stram: %v", err)
			return output.FLB_ERROR
		}
	}

	if config.client != nil {
		if err := config.client.Close(); err != nil {
			log.Printf("Couldn't close managed writer client: %v", err)
			return output.FLB_ERROR
		}
	}

	return output.FLB_OK
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	log.Print("[multiinstance] Unregister called")
	output.FLBPluginUnregister(def)
}

func main() {
}
