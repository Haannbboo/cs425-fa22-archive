

class IdunnoClient:

    def run(self):
        print()
        print("IDunno version 0.0.1")

        while True:
            command = input(">>> ")
            argv = command.split()
            if len(argv) == 0:
                continue
            elif argv[0] == "train" and len(argv) > 1:
                #send message to all worker directly? 
                continue
            elif argv[0] == "if" and len(argv) > 2:
                #inference specific task by specific model in given batch size
                continue
            elif argv[0] == "state" and len(argv) > 1:
                #show the job's state (demo C1)
                continue
            elif argv[0] == "result" and len(argv) > 1:
                #show the result of given job (demo C4)
                continue
            elif argv[0] == "assign":
                #show the current set of VMs assigned to each job (demo C5)
                continue
            else:
                print(f"[ERROR] Invalid command: {command}")
    