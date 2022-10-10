function main(input)
    if string.find(input.Data, "aid_6", 0, true) ~= nil then
        return input
    else
        return {Data = "", FilterOut = true}
    end
end