class Parser():
    """ Parse received requests from client """

    def __init__(self, request):
        self.type_indicators = ("+", "*", ":", "$")
        self.requests_list = list(request.decode().split())
        self.request_type = self.requests_list[0]

    def bulk_array(self):
        buffer = []
        s = ''.join(self.requests_list)
        count = s.count("*")

        def buffer_requests():
            temp_list = []
            for i in self.requests_list:
                if "*" in i:
                    if len(temp_list) > 0:
                        buffer.append(temp_list)
                        temp_list = []
                    temp_list.append(i)
                else:
                    temp_list.append(i)
            buffer.append(temp_list)

        # if count > 1:
        #    buffer_requests()
        # else:
        buffer.append(self.requests_list)
        commands = self.parse_commands(buffer)
        return commands

    def parse_commands(self, requests):
        commands = []
        for request in requests:
            temp = []
            for i in request:
                if not i.startswith(self.type_indicators):
                    temp.append(i)
            commands.append(temp)
        return commands

    def bulk_string(self):
        buffer = []
        l = self.requests_list.decode().split()
        buffer.append(l)
        commands = self.parse_commands(buffer)
        return commands


test1 = b'*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n'

test2 = b'*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n'
test3 = b'$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n'

parser = Parser(test1)
