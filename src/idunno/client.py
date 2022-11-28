

class IdunnoClient:

    def run(self):
        print()
        print("IDunno version 0.0.1")

        while True:
            command = input(">>> ")
            argv = command.split()
            if len(argv) == 0:
                continue
            else:
                print(f"[ERROR] Invalid command: {command}")
    