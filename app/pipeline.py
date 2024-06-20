r1 = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
r2 = b"*1\r\n$4\r\nPING\r\n*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
r3 = b'$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n'


def buffer_requests(r):
    buffer = []
    request = r.decode()  # str(r1)[2:-1]
    requestArray = [i for i in request]
    # print(request)
    # print(requestArray)
    temp = []
    if requestArray[0] == '*' and temp == []:
        len_array = int(requestArray[1])
        # print(f"Actual len: {len(request)}")
        # print(f"Actual length: {len(requestArray)}")
        # print(f"calculated length: {len_array}")
        # for i in range(len_array):

    temp_list = []
    for i in range(len(requestArray)+1):
        try:
            next = requestArray[i+1] if len(requestArray) >= i+1 else None
        except IndexError:
            next = None
        try:
            value = requestArray[i]
        except IndexError:
            value = None
            break
        # print(f"Value: {value}")
        if value == '*':
            # print(f"Temp list init: {temp_list}")
            # print(f"Next: {next}")
            if next == "\r":
                temp_list.append(value)
                # print(f"Temp list 1: {temp_list}")
            elif next != "\r":
                if len(temp_list) > 0:
                    buffer.append(''.join(temp_list).encode())
                    # print(f"Length of buffer: {len(buffer)}")
                    # print(buffer)
                    temp_list = []
                temp_list.append(value)
                # print(f"Temp list 2: {temp_list}")
        else:
            temp_list.append(value)
            # print(f"Temp list 3: {temp_list}")
    buffer.append(''.join(temp_list).encode())
    # print(f"Length of buffer: {len(buffer)}")
    # print(buffer)
    # for i in buffer:
    #    print(len(i))
    return buffer


a = buffer_requests(r3)
print(a)
