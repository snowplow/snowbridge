function main(input)
    new = string.gsub(input.Data, "aid_", "test_")
    return {Data = new}
end