# Map Reduce Recommendation Links

This is a Map Reduce solution for recommendation links or friend suggestions.
This project has been managed by Maven and uses Apache Spark API


#### 1) Usage
This is the command to execute the application.
The first argument is the path to the data set:

```
FriendRecommendationMR <project-path>/dataset.txt
```

#### 2) File Result
When the execution has finished, a file with, with the steps and result, is generated in the following folder path: 

```
<project-path>/result_recommendation.txt
```

#### 2) Interpreting the File Result

##### PHASE 1
Reading data set lines:
    
    HOW TO READ: <USER_ID><TAB><FRIEND_1><,><FRIEND_2><,><FRIEND_3><,>...

	USER_ID=100	200,300,400,500,600
	USER_ID=200	100,300
	USER_ID=300	100,200,400,700,600
	USER_ID=400	100,300
	USER_ID=500	100
	USER_ID=600	100,300
	USER_ID=700	300
	
##### PHASE 2 
This is the Mapper returning phase showing all possible friendships. Those with -1 are direct friendship:
	
	USER_ID=100	 VALUE=(200,-1)
	USER_ID=100	 VALUE=(300,-1)
	USER_ID=100	 VALUE=(400,-1)
	USER_ID=100	 VALUE=(500,-1)
	USER_ID=100	 VALUE=(600,-1)
	USER_ID=200	 VALUE=(300,100)
	USER_ID=300	 VALUE=(200,100)
	...
	
##### PHASE 3 
Regroup all possible friendships values in the same key(USER_ID): 

	USER_ID=100	 VALUES=[(200,-1), (300,-1), (400,-1), (500,-1), (600,-1), (300,200), (200,300), (400,300), (700,300), (600,300), (300,400), (300,600)]
	USER_ID=600	 VALUES=[(200,100), (300,100), (400,100), (500,100), (100,300), (200,300), (400,300), (700,300), (100,-1), (300,-1)]
	USER_ID=300	 VALUES=[(200,100), (400,100), (500,100), (600,100), (100,200), (100,-1), (200,-1), (400,-1), (700,-1), (600,-1), (100,400), (100,600)]
    ...
   
   
#### FINAL RESULT ################# 
A line shows one USER_ID, one RECOMMENDATION and the set of common friends that allowed this recommendation.

	USER_ID=100	 RECOMMENDATION=700 (1: [300]) 
	USER_ID=600	 RECOMMENDATION=400 (2: [100, 300]) 500 (1: [100]) 200 (2: [100, 300]) 700 (1: [300]) 
	USER_ID=300	 RECOMMENDATION=500 (1: [100]) 
	USER_ID=200	 RECOMMENDATION=400 (2: [100, 300]) 500 (1: [100]) 600 (2: [100, 300]) 700 (1: [300]) 
	USER_ID=700	 RECOMMENDATION=400 (1: [300]) 100 (1: [300]) 200 (1: [300]) 600 (1: [300]) 
	USER_ID=400	 RECOMMENDATION=500 (1: [100]) 200 (2: [100, 300]) 600 (2: [100, 300]) 700 (1: [300]) 
	USER_ID=500	 RECOMMENDATION=400 (1: [100]) 200 (1: [100]) 600 (1: [100]) 300 (1: [100]) 


