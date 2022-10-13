function main(input)
    input["PartitionKey"] = input["Data"]["event_id"]

    return input
end