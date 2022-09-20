function main(input) {

    if (!input.Data.includes("test-data1")) {
        return {
            FilterOut: true
        };
    }

    return input;
}


function main(input)
    if input.Data:find("test-data1") ~= nil then
        return input
    else
        return {Data = "THIS IS FOUND ONCE", FilterOut = true}
    end
end