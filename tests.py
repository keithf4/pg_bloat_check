from pg_bloat_check import convert_to_bytes

### This section tests the convert_to_bytes() function ###

def test_convert_to_bytes(input_val,expected_val):
    return_val = convert_to_bytes(input_val)
    if return_val != expected_val:
        print("Test failed for {} -- Expected {} but got {}".format(input_val,expected_val,return_val))

# Test numeric input
# print("Testing int")
test_convert_to_bytes(1,1)
test_convert_to_bytes(1000,1000)
test_convert_to_bytes(1000000,1000000)
test_convert_to_bytes(1000000000,1000000000)
test_convert_to_bytes(1000000000000,1000000000000)

# Test basic string input
# print("Testing str")
test_convert_to_bytes("1",1)
test_convert_to_bytes("1000",1000)
test_convert_to_bytes("1000000",1000000)
test_convert_to_bytes("1000000000",1000000000)
test_convert_to_bytes("1000000000000",1000000000000)

# Test permutations of alphas
test_convert_to_bytes("1kb",1024)
test_convert_to_bytes("1Kb",1024)
test_convert_to_bytes("1KB",1024)

# Test really big numbers
test_convert_to_bytes("1MB",1048576)
test_convert_to_bytes("1GB",1073741824)
test_convert_to_bytes("1TB",1099511627776)
test_convert_to_bytes("1PB",1125899906842624)
test_convert_to_bytes("1EB",1152921504606846976)
test_convert_to_bytes("1ZB",1180591620717411303424)

# Test invalid input
test_convert_to_bytes("q","q")
test_convert_to_bytes("-1","-1")
test_convert_to_bytes("1 KB",1)

### End of convert_to_bytes() test ###
