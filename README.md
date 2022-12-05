# CS425 MP4: IDunno

# Build

In this projects, we have used some libaraies to help us pretrain the model. We first need to run
```sh
bash install.sh
```
to download required libaraies.

# Run

We need to first run the DNS
```sh
python3 idunno.py dns
```
Then, we can run the client, which would also turn on the coordinator, standby and worker
```sh
python3 idunno.py client
```
# Commands we have
After run the project, we have following commands we can input after ">>>" in the terminal 

To train the model
```sh
train model_name
```
For now, we can only use the model from Microsoft in hugging face

To upload inference date to simple distributed file system
```sh
upload input directory n
```
where n is the number of files of input directory we want 

To infrerece,
```sh
inference model_name input_directory batch_size
```

To see the curry query rate of models and the number of queries processes so far for each models
```sh
C1
```

To see current processing time of a query of models
```sh
C2
```

To see the current set of VMs assigned to each model/job
```sh
C5
```

