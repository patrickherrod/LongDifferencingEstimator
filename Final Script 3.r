# install necessary packages, to be run start of the first session
# install.packages("readxl")
# install.packages("plm")
# install.packages("plyr")
# install.packages("foreach")
# install.packages("doParallel")
# install.packages("car")
# install.packages("lmtest")

# Load dependencies
library(readxl)
library(plm)
library(plyr)
library(parallel)
library(Formula)
library(iterators)
library(foreach)
library(doParallel)
library(lmtest)

options(warn=-1)	# Command to turn off warning messages, so that output is more easily readable

# Loading excel file into R
# !! To get this line of code working, ensure you have the correct path to your file
# !! On Windows, highlight then right click file, click "Properties", "Info" tab, "filepath:" copy paste into quotes 
# !! On Mac OSx, right click file, "Get Info", under "General", "Where:" copy paste into quotes
# !! Left my filepath as an example
my_data <- read_excel("/Users/Administrator/Desktop/Final 11 Year Data.xlsx")


#### ----- beginning of script logic ----- ####

# set variables before processing

# Functionality to prompt user for program variables
# readinteger <- function() {
# 	n <- ''
# 	while (n == ''){
# 		n <- readline(prompt="Please enter the lag for your test: ")
# 		if(!grepl("^[0-9]+",n)) {
# 			cat("Error: This program only accepts 1 digit values")
# 			n <- ''
# 		}
# 	}
# 	return(as.integer(n))
# }
# lag <- readinteger()
 
# Calculate the number of host system cores
no_cores <- detectCores() - 1
 
# Initiate parallel computing cluster
cl <- makeCluster(no_cores)
setDefaultCluster(cl)
registerDoParallel(cl)


lag <- 8 # 8-year lag
iterations <- 3 # LD iterations
iv_var <- "ML"	# independent variable definition

# Functionality to take command line arguments to set variable from a terminal
# args <- commandArgs(trailingOnly = TRUE)
# # cat(args, sep = "\n")
# if (length(args) > 0) {
# 	for (i in 1:length(args)) {
# 		if (args[i] == '-lag' || args[i] == '-l') {
# 			lag <- as.integer(args[i+1])
# 		} else if(args[i] == '-iterations' || args[i] == '-it') {
# 			iterations <- args[i+1]
# 		} else if(args[i] == '-IV' || args[i] == '-iv') {
# 			if(args[i+1] == 'BL' || args[i+1] == 'bl') { 
# 				iv_var <- "BL"
# 			}
# 		}
# 	}
# }

# print varibles to use in Algorithm
cat("Lag: ", lag, "\n", sep='')
cat("LD iterations: ", iterations, "\n", sep='')
cat("IV : ", iv_var, "\n", sep='')

lag_vars <- c(iv_var,"Q","RnD","CAPEX","SALE","OIBD","TANG","ERP","RIR","DSP","TSP","RnDD") # selecting the correct columns to diff from tibble
curr_vars <- c(iv_var,"TAXR","RGDP","ERD","GERD","CONGD") # select current variables to diff

# --- Variables for different plm formulas ---
if(iv_var == "ML") {
	plm.dl <- as.formula("D_ML ~ D_LML - 1")
	plm.Rs <- "D_ML ~ D_LML"
} else {
	plm.dl <- as.formula("D_BL ~ D_LBL - 1")
	plm.Rs <- "D_BL ~ D_LBL"
}
for (i in 1:lag) {
	plm.Rs <- paste(plm.Rs, " + R",i,sep='') 	# build Residual formula
}
plm.Rs <- paste(plm.Rs, " - 1", sep='') # append "-1" to Residual formula
plm.Rs <- as.formula(plm.Rs)
D <- " + D_Q + D_RnD + D_CAPEX + D_SALE + D_OIBD + D_TANG + D_ERP + D_RIR + D_DSP + D_TSP + D_RnDD + D_TAXR + D_RGDP + D_ERD + D_GERD + D_CONGD - 1" # append necessary columns to Long Differencing formula

keys <- unique(my_data$CUSIP) # store all unique CUSIPs in a list

# Function to separate data by company
split_companies <- function(x, data) {
	return (data[data$CUSIP == x,])
}

# Differencing function
diff_func <- function(y) {
	diff(y, lag)
}

# Helper function to handle special column cases and determine how to diff the rows for each company
wrapper <- function(x) {
	a <- head(x[lag_vars], -1) # remove the last row for lag diff of ML-TSP
	res <- apply(a, 2, diff_func) # perform diff on lag_vars, excluding the last row
	b <- tail(x[curr_vars], -1) # remove first row prior to diff cur_vars
	res2 <- apply(b, 2, diff_func) # perform diff on lag vars (ML-RGDP columns), excluding the first row
	index <- tail(x[,1:2],-(lag+1)) # keep, but shrink index columns
	res <- cbind(index, res, res2) # join (and order) all columns together
	return (res)
}

# Create special case of instrumental variable "zero"
create_iv0 <- function(d) {
	if(iv_var == "ML") {	# determine the selected iv
		ND_ML <- sapply(d$D_LML, function(z, slope) { 	# iterate through company data by D_ML column values
			return (z*slope)		# calculate D_ML * regression slope
		}, regres_slope)
		IV0 <- c(NA, head(ND_ML, -1)) 	# create separate IV column, removing the first row
		res <- cbind(ND_ML, IV0) 		# store both ND_ML and IV0 columns
	} else {
		ND_BL <- sapply(d$D_LBL, function(z, slope) {	# iterate through company data by D_BL column values
			return (z*slope)		# calculate D_BL * regression slope
		}, regres_slope)

		IV0 <- c(NA, head(ND_BL, -1))	# create separate IV column, removing the first row
		res <- cbind(ND_BL, IV0)		# store both ND_ML and IV0 columns
	}
	return (res)	# return caculated columns
}

# Function to extract and store the regression coefficients
# this function is dependant on the order of the data's columns
est_coefficients <- function(p) {
	iv <- unname(p[1])			# strip each value of it's "name" and store in a variable
	d_q <- unname(p[2])
	d_rnd <- unname(p[3])
	d_capex <- unname(p[4])
	d_sale <- unname(p[5])
	d_oidb <- unname(p[6])
	d_tang <- unname(p[7])
	d_erp <- unname(p[8])
	d_rir <- unname(p[9])
	d_dsp <- unname(p[10])
	d_tsp <- unname(p[11])
	d_rndd <- unname(p[12])
	d_taxr <- unname(p[13])
	d_rgdp <- unname(p[14])
	d_erd <- unname(p[15])
	d_gerd <- unname(p[16])
	d_congd <- unname(p[17])
	est_coeff <- data.frame(cbind(	iv, 	# add variales to data frame for ease-of-acces
									d_q, 
									d_rnd,
									d_capex,
									d_sale,
									d_oidb,
									d_tang,
									d_erp,
									d_rir,
									d_dsp,
									d_tsp,
									d_rndd,
									d_taxr,
									d_rgdp,
									d_erd,
									d_gerd,
									d_congd))
	return (est_coeff)		# return resulting data frame
}

# Function to calculate all residuals for one row of company data
row_residuals <- function(row) {
	num_resids <- c(1:lag)
	res <- laply(num_resids, calc_residuals, row, my_data, iv_var, est_coeff) # calculate the Residual and iterate to calculate all residuals

	if(iv_var == "ML") {
		res <- c(row$CUSIP, row$Year, row$D_ML, row$D_LML, res)	# add CUSIP, Year, D_ML, and D_LML to calulated residual
		res_names <- c("CUSIP", "Year", "D_ML", "D_LML")		# name the columns
	} else {
		res <- c(row$CUSIP, row$Year, row$D_BL, row$D_LBL, res)
		res_names <- c("CUSIP", "Year", "D_BL", "D_LBL")
	}
	for (i in 1:lag) {
		res_names <- c(res_names, paste("R", i, sep=''))	# name each calculated residual
	}
	names(res) <- res_names		# set the column names
	return (res)			# return the result
}


# Function to calculate a single residual
calc_residuals <- function(r, row, my_data, iv_var, est_coeff) {
	r_lag <- 1+r 		# set lag based on which R to calculate
	curr_data_row <- my_data[my_data$CUSIP == row$CUSIP & my_data$Year == row$Year,]		# get raw data row that matches the row passed to this function
	lag_year_data_row <- my_data[my_data$CUSIP == row$CUSIP & my_data$Year == (row$Year-1),]	# get raw data row the that matches one year prior to the row passed to this function
	subtrahend_data_row <- my_data[my_data$CUSIP == row$CUSIP & my_data$Year == (row$Year-r_lag),] # get raw data row that mathces lagged years prior to row passed to this function (subtrahend for lagged coeffs)
	
	# Calculate and store lambda - delta15 (values needed to calculate residual)
	# Difference the data values, then multiply with corresponding regression coefficients
	if(iv_var == "ML") {
		lambda <- est_coeff$iv*(lag_year_data_row$ML - subtrahend_data_row$ML)
	} else {
		lambda <- est_coeff$iv*(lag_year_data_row$BL - subtrahend_data_row$BL)
	}
	delta0 <- est_coeff$d_q*(lag_year_data_row$Q - subtrahend_data_row$Q)
	delta1 <- est_coeff$d_rnd*(lag_year_data_row$RnD - subtrahend_data_row$RnD)
	delta2 <- est_coeff$d_capex*(lag_year_data_row$CAPEX - subtrahend_data_row$CAPEX)
	delta3 <- est_coeff$d_sale*(lag_year_data_row$SALE - subtrahend_data_row$SALE)
	delta4 <- est_coeff$d_oidb*(lag_year_data_row$OIBD - subtrahend_data_row$OIBD)
	delta5 <- est_coeff$d_tang*(lag_year_data_row$TANG - subtrahend_data_row$TANG)
	delta6 <- est_coeff$d_erp*(lag_year_data_row$ERP - subtrahend_data_row$ERP)
	delta7 <- est_coeff$d_rir*(lag_year_data_row$RIR - subtrahend_data_row$RIR)
	delta8 <- est_coeff$d_dsp*(lag_year_data_row$DSP - subtrahend_data_row$DSP)
	delta9 <- est_coeff$d_tsp*(lag_year_data_row$TSP - subtrahend_data_row$TSP)
	delta10 <- est_coeff$d_rndd*(lag_year_data_row$RnDD - subtrahend_data_row$RnDD)
	subtrahend_data_row <- my_data[my_data$CUSIP == row$CUSIP & my_data$Year == (row$Year-r),] #reset subtrahend for non-lagged coefficients
	if(iv_var == "ML") {
		ml_sub <- (curr_data_row$ML - subtrahend_data_row$ML)
	} else {
		bl_sub <- (curr_data_row$BL - subtrahend_data_row$BL)
	}
	delta11 <- est_coeff$d_taxr*(curr_data_row$TAXR - subtrahend_data_row$TAXR)
	delta12 <- est_coeff$d_rgdp*(curr_data_row$RGDP - subtrahend_data_row$RGDP)
	delta13 <- est_coeff$d_erd*(curr_data_row$ERD - subtrahend_data_row$ERD)
	delta14 <- est_coeff$d_gerd*(curr_data_row$GERD - subtrahend_data_row$GERD)
	delta15 <- est_coeff$d_congd*(curr_data_row$CONGD - subtrahend_data_row$CONGD)
	
	# Calculate residual, subtracting terms from lagged ml or bl
	if(iv_var == "ML") {
		R <- ml_sub - lambda - delta0 - delta1 - delta2 - delta3 - delta4 - delta5 - delta6 - delta7 - delta8 - delta9 - delta10 - delta11 - delta12 - delta13 - delta14 - delta15
	} else {
		R <- bl_sub - lambda - delta0 - delta1 - delta2 - delta3 - delta4 - delta5 - delta6 - delta7 - delta8 - delta9 - delta10 - delta11 - delta12 - delta13 - delta14 - delta15
	}
	return (R)	# return R result
}

# General function to create an instrumental variable
create_iv <- function(data) {
	d <- adply(data, 1, create_nd, regres_slope, .expand=F)		# iterate over company data by row and calculate ND
	if(iv_var == "ML") {
		IV <- c(NA, head(d$ND_ML, -1))		# create IV column removing the first row
	} else {
		IV <- c(NA, head(d$ND_BL, -1))
	}
	d <- cbind(d, IV)			# combine N and IV columns
	return (d)			# return result
}

# Function to create ND varible
create_nd <- function(z, slope) {
	if(iv_var == "ML") {
		D_LML_term <- slope * z$D_LML
		ND_ML <- D_LML_term
	} else {
		D_LBL_term <- slope * z$D_LBL
		ND_BL <- D_LBL_term
	}
	RS <- lapply(1:lag, function(i) {				# iterate over Rs
		return (unname(regres$coefficients[i+1]) * z[[paste("R",i,sep='')]])	# multiply regression coefficient and corresponding R value
	})
	RS <- Reduce("+",RS)		# reduce Rs to one value by summation
	if(iv_var == "ML") {		# finish calculation and return data frame
		ND_ML <- ND_ML + RS
		return (data.frame(ND_ML))
	} else {
		ND_BL <- ND_BL + RS
		return (data.frame(ND_BL))
	}
}

# --- Main processing --- #
clusterExport(cl, list("lag", "lag_vars", "curr_vars", "diff_func", "cl"), envir=environment())

split_data <- parLapply(cl, keys, split_companies, my_data) # create a tibble(very much like a table) with data separated by company
result <- parLapply(cl, split_data, wrapper) # process each company tibble and return resulting values, to view results in r studio simply type "results"
result <- do.call(rbind, result) # combine the tibbles
if(iv_var == "ML") {
	colnames(result) <- c("CUSIP","Year","D_LML","D_Q","D_RnD","D_CAPEX","D_SALE","D_OIBD","D_TANG","D_ERP","D_RIR","D_DSP","D_TSP","D_RnDD","D_ML","D_TAXR","D_RGDP","D_ERD","D_GERD","D_CONGD") # give result data appropriate column names
} else {
	colnames(result) <- c("CUSIP","Year","D_LBL","D_Q","D_RnD","D_CAPEX","D_SALE","D_OIBD","D_TANG","D_ERP","D_RIR","D_DSP","D_TSP","D_RnDD","D_BL","D_TAXR","D_RGDP","D_ERD","D_GERD","D_CONGD") # give result data appropriate column names
}
pdata <- plm.data(result, c("CUSIP", "Year")) # cast data to pdata for panel regression
regres <- plm(plm.dl,  data=pdata, model="pooling") # run regression
regres_slope <- unname(regres$coefficients[1])		# remove extra infor and store regression slope
split_data <- parLapply(cl, keys, split_companies, result) # split data from result table NOT my_data
IV0 <- adply(split_data, 1, create_iv0, .parallel=T, .paropts=.(.export=c("iv_var", "regres_slope")))$IV0	# compute ND_ML and IV0 variables
result <- cbind(result, IV0)	# add ND_ML and IV0 columns to results table
pdata <- plm.data(result, c("CUSIP", "Year")) # cast data to pdata for panel regression
if(iv_var == "ML") {
	L <- paste("D_ML ~ IV", 0, sep='')
} else {
	L <- paste("D_BL ~ IV", 0, sep='')
}
plm.LD <- as.formula(paste(L,D,sep=''))			# build long differencing formula
LD_0 <- plm(plm.LD, data=pdata, model="pooling")		# run first long difference
est_coeff <- est_coefficients(LD_0$coefficients)		# store long difference coefficients
residuals <- adply(split_data, 1, function(tibble) {		# iterate over all data to calculate residuals
	return (adply(tibble, 1, row_residuals, .expand=F, .id=NULL))	# iterate over each companies data by row
}, .expand=F, .id=NULL, .parallel=T, .paropts=.(.export=c("row_residuals", "calc_residuals", "my_data", "iv_var", "est_coeff")))	# set parameters and variables for outer iteration to run in parallel
assign("RS_1", residuals)				# create variable for first round of residual values
pdata <- plm.data(residuals, c("CUSIP", "Year"))
regres <- plm(plm.Rs, data=pdata, model="pooling")		# run regression over first residuals
cat("Iteration: 1", "\n", sep="")
print(regres)
regres_slope <- unname(regres$coefficients[1])		# store new regression slope
split_residuals <- parLapply(cl, keys, split_companies, residuals)		# split residuals by CUSIP
for (i in 1:iterations) {
	IV <- adply(split_residuals, 1, create_iv, .expand=F, .id=NULL, .parallel=T, .paropts=.(.export=c("create_nd", "iv_var", "regres_slope", "regres")))$IV 	# calculate IV in parallel
	assign(paste("IV", i, sep=''), IV)		# store IV according to the iteration number
	result <- cbind(result, IV)			# add IV to result table
	colnames(result)[length(colnames(result))] <- paste("IV",i,sep='')		# rename current IV column according to iteration number
	pdata <- plm.data(result, c("CUSIP", "Year")) 	# cast data to pdata for panel regression
	if(iv_var == "ML") {
		L <- paste("D_ML ~ IV", i, sep='')
	} else {
		L <- paste("D_BL ~ IV", i, sep='')
	}
	plm.LD <- as.formula(paste(L,D,sep=''))
	LD <- plm(plm.LD, data=pdata, model="pooling")			# perform Long Difference
	assign(paste("LD_",i,sep=''), LD)						# create LD variable for current iteration
	est_coeff <- est_coefficients(LD$coefficients)			# store LD coefficients
	residuals <- adply(split_data, 1, function(tibble) {					# iterate over data by company to calculate residuals
		return (adply(tibble, 1, row_residuals, .expand=F, .id=NULL))		# iterate over each row in company data to calculate residual
	}, .expand=F, .id=NULL, .parallel=T, .paropts=.(.export=c("row_residuals", "calc_residuals", "my_data", "iv_var", "est_coeff")))		# set parameters and variables for outer iteration to run in parallel
	assign(paste("RS_",i,sep=''), residuals)			# store residual values for current iteration
	pdata <- plm.data(residuals, c("CUSIP", "Year"))
	regres <- plm(plm.Rs, data=pdata, model="pooling")		# run regression on Residuals
	assign(paste("regres_",i,sep=''), regres)			# store residual regression for this iteration
	cat("Iteration: ", i, "\n", sep="")
	print(regres)
	regres_slope <- unname(regres$coefficients[1])		# store iteration regression slope
}


# White's Correction
coeftest(LD_3, vcov = pvcovHC(LD_3, method = "white2", type = "HC3"))

# Stop and release cluster
stopImplicitCluster()
stopCluster(cl)