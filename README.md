# LongDifferencingEstimator
Capitol Structure, Speed of Adjustment, Long Differencing Estimator

A project I worked on with a friend to help him with his Master of Fincance thesis.

Subject Matter:

This project is an unbalanced panal data long differencing model that utilizes instrumental variables in iterated two stage least squares to estimate the speed of adjustment of capitol structure.

The inputs consist of company data over 10+ years, and many different companies.  The data for the final study consisted of 90,000 records to be processed.

Technical:

This project was written using R.  I am by no strech of the imagination an R expert, but when asked to help this tool came to mind.  I am a programmer with a little experience in R and SPSS, so I naturallly felt more comfortable with R.

The requirements for this tool were that it needed to process 90,000 records, n-times.  N could be as large as 20.  Paralellization was necessary for runtime, but development time as well.  As we gave the script larger and larger inputs I didn't have the time wait to analyze and resolve issues.  We used Amazon EC2 xomputed optimized instances for our tests and final study.  The low memory, 16 core option suited our price range and finished our the final calculations in about 30 minutes.

The study was a success.  My friend recieved an 'A' on his thesis.  If you're interested in reading the paper I would be happy to provide a copy. 
