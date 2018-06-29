
#### data analysis with Bigquery and R ####
# Written by Jihye Lee 
# from 2018-06-22 to 

#### Bigquery data extraction #### 
library(bigrquery)

# project, dataset 이름 정의 
projectName <- "ga360-export" 
datasetName <- "ga_sessions_web_view"

ds <- bq_dataset(projectName, datasetName)

# 예시용 
sql <- "
  SELECT fullVisitorId , visitId 
  FROM `v_visit`
  WHERE TABLE_DATE BETWEEN @startDate AND @endDate;"

# 실전용; device 데이터 추출 

##sql <- "
  ##SELECT TABLE_DATE, fullVisitorId, browser, operatingSystem, operatingSystemVersion, mobileDeviceBranding, mobileDeviceModel, deviceCategory 
  ##FROM `v_device` 
  ##INNER JOIN ``v_customDimensions_pivot`
  ##WHERE TABLE_DATE BETWEEN @startDate AND @endDate;"

# mobiledevice에서 떼어내서 tabletdivicemodel 다시 만들어야 함

sql <- "
SELECT
  de.TABLE_DATE,
  de.operatingSystem,
  	CASE WHEN de.operatingSystem in ('Android') then 1
  	CASE WHEN de.deviceCategory in ('iOS') then 2
	CASE WHEN de.deviceCategory in ('Blackberry') then 3
	CASE WHEN de.deviceCategory in ('Chrome OS') then 4
	CASE WHEN de.deviceCategory in ('Linux') then 5
	CASE WHEN de.deviceCategory in ('Macintosh') then 6
	CASE WHEN de.deviceCategory in ('Windows') then 7
	CASE WHEN de.deviceCategory in ('(not set)') then 0
  de.operatingSystemVersion,
  de.browser,
  de.browserVersion,
  de.screenResolution,
  de.deviceCategory,
	CASE WHEN de.deviceCategory in ('mobile') then 1
	CASE WHEN de.deviceCategory in ('tablet') then 2
		ELSE 0 end as isMobile,
  de.mobileDeviceBranding,
	CASE WHEN de.mobileDeviceBranding in ('(not set)') then 0
	CASE WHEN de.mobileDeviceBranding in ('NA') then 0
	CASE WHEN de.mobileDeviceBranding in ('Alcatel') then 1
	CASE WHEN de.mobileDeviceBranding in ('Apple') then 2
	CASE WHEN de.mobileDeviceBranding in ('Blackberry') then 3
	CASE WHEN de.mobileDeviceBranding in ('Google') then 4
	CASE WHEN de.mobileDeviceBranding in ('Huawei') then 5
	CASE WHEN de.mobileDeviceBranding in ('KT Tech') then 6
	CASE WHEN de.mobileDeviceBranding in ('Lava') then 7
	CASE WHEN de.mobileDeviceBranding in ('LeEco') then 8
	CASE WHEN de.mobileDeviceBranding in ('LG') then 9
	CASE WHEN de.mobileDeviceBranding in ('Luna') then 10
	CASE WHEN de.mobileDeviceBranding in ('OPPO') then 11
	CASE WHEN de.mobileDeviceBranding in ('Pantech') then 12
	CASE WHEN de.mobileDeviceBranding in ('Samsung') then 13
	CASE WHEN de.mobileDeviceBranding in ('Sony') then 14
	CASE WHEN de.mobileDeviceBranding in ('TCL') then 15
	CASE WHEN de.mobileDeviceBranding in ('TG & Company') then 16
	CASE WHEN de.mobileDeviceBranding in ('Xiaomi') then 17
  de.mobileDeviceModel,
  CASE WHEN cd.customDimensions_18 in ('A','B','C') then 1
		ELSE 0 end as isRegister,
  CASE WHEN cd.customDimensions_18 in ('A') then 1
		ELSE 0 END AS isApply
FROM `ga_sessions_web_view.v_device` AS de
JOIN `ga_sessions_web_view.v_customDimensions_pivot` AS cd
	on de.sessionId = cd.sessionId
WHERE de.TABLE_DATE BETWEEN @startDate AND @endDate
AND cd.TABLE_DATE BETWEEN @startDate AND @endDate
ORDER BY de.TABLE_DATE, de.sessionId"

#tb=table  
tb <- bq_dataset_query(ds,
                       query = sql,
                       parameters = list(startDate = "20180501" , endDate = "20180531"),
                       billing = NULL,
                       quiet = NA)

result <- bq_table_download(tb, 
                            page_size = 200 , 
                            start_index = 1 , 
                            max_connections = 5, 
                            max_results = 20000)

print(result)

#### Data wrangling #### 
#ref https://rpubs.com/jmhome/R_data_processing

data <- result

# variables
str(data)
head(data)

# data backup
dt <- data
dt <- read.csv("ga360-data-20180621-2.csv")

library(readr)
dt <- read_csv("ga360-data-20180621-2.csv")

# data 결측치 전부 제거 (신중함 필요)
sum(is.na(dt))
dt.na <- na.omit(dt)

# operatingSystem이 NA인 데이터만 출력
#library(dplyr)
#dt %>% filter(is.na(operatingSystem))   # operatingSystem이 NA인 데이터만 출력
#df_nomiss <- df %>% filter(!is.na(operatingSystem))   # operatingSystem 결측치 제거한 데이터 만들기 

# chr to num 
#dt$isRegister <- as.character(dt$isRegister)
#dt$isApply <- as.character(dt$isApply)

dt$mobileDeviceModel <- as.character(dt$mobileDeviceModel)

# data distribution (EDA)
hist(dt$isRegister)
hist(dt$isApply)

# categorical variables의 분포 알아보기 
levels(dt$mobileDeviceBranding)
levels(dt$browser)
levels(dt$operatingSystem)

# 접수와 승인의 갯수 알아보기 
length(which(dt$isRegister==1)) #isregister에서 1의 갯수 
length(which(dt$isRegister==0))
length(which(dt$isApply==1)) #isapply에서 1의 갯수 
length(which(dt$isApply==0))

length(which(dt$isMobile==0)) #ismobile에서 0의 갯수 
length(which(dt$isMobile==1))

length(which(dt$TABLE_DATE=="20180531"))
length(which(dt$TABLE_DATE=="20180515")) #해당 날짜에 데이터 존재 유무 확인 
 
# factor to numeric (보류) 

dt$broswerVersion <- as.numeric(dt$broswerVersion)
dt$operatingSystemVersion <- as.numeric(dt$broswerVersion)
dt.na.h$mobileDeviceBranding <- as.character(dt.na.h$mobileDeviceBranding)


#nuemric to categorical (Sample code)
df$category <- cut(df$a, breaks=c(-Inf, 0.5, 0.6, Inf), labels=c("low","middle","high"))


# -----카테고리 데이터로 바꾸기 (1,2,3,4...) ~굳이 필요한 코드는 아닌듯~----- # 

dt.na.h$operatingSystem <- 
  ifelse(dt.na.h$operatingSystem %in% 'Android', 1,
  ifelse(dt.na.h$operatingSystem %in% 'iOS', 2, 
  ifelse(dt.na.h$operatingSystem %in% 'Blackberry', 3, 
  ifelse(dt.na.h$operatingSystem %in% 'Chrome OS', 4,
  ifelse(dt.na.h$operatingSystem %in% 'Linux', 5,
  ifelse(dt.na.h$operatingSystem %in% 'Macintosh', 6,
  ifelse(dt.na.h$operatingSystem %in% 'Windows', 7,
  	0)))))))


dt.na.h$mobileDeviceBranding <-
  ifelse(dt.na.h$mobileDeviceBranding %in% 'Alcatel', 1,
  ifelse(dt.na.h$mobileDeviceBranding %in% 'Apple', 2, 
  ifelse(dt.na.h$mobileDeviceBranding %in% 'Blackberry', 3, 
  ifelse(dt.na.h$mobileDeviceBranding %in% 'Google', 4,
  ifelse(dt.na.h$mobileDeviceBranding %in% 'Huawei', 5,
  ifelse(dt.na.h$mobileDeviceBranding %in% 'KT Tech', 6,
  ifelse(dt.na.h$mobileDeviceBranding %in% 'Lava', 7,
  ifelse(dt.na.h$mobileDeviceBranding %in% 'OPPO', 8,
  ifelse(dt.na.h$mobileDeviceBranding %in% 'Pantech', 9,
  ifelse(dt.na.h$mobileDeviceBranding %in% 'Samsung', 10, 
  ifelse(dt.na.h$mobileDeviceBranding %in% 'Sony', 11, 
  ifelse(dt.na.h$mobileDeviceBranding %in% 'TCL', 12,
  ifelse(dt.na.h$mobileDeviceBranding %in% 'TG & Company', 13,
  ifelse(dt.na.h$mobileDeviceBranding %in% 'Xiaomi', 14,
  ifelse(dt.na.h$mobileDeviceBranding %in% 'Windows', 15,
  	0)))))))))))))))

dt$deviceCategory <-
  ifelse(dt.na.h$deviceCategory %in% 'mobile', 1,
  ifelse(dt.na.h$deviceCategory %in% 'desktop', 2, 
  ifelse(dt.na.h$deviceCategory %in% 'tablet', 3,
  	0)))

# ----------------------------------------------------------------------- # 


# 컬럼 새로 만들기 
library(dplyr)

dt <- mutate %>% mutate(mobileDeviceModel == Windows)

mobile <- dt %>% filter(deviceCategory == mobile)  
desktop <- dt %>% filter(deviceCategory == desktop)  
tablet <- dt %>% filter(deviceCategory == tablet)

# use quantiles for cut https://stackoverflow.com/questions/40380112/categorize-continuous-variable-with-dplyr

# 카이제곱검정
# 11p~ http://contents.kocw.net/KOCW/document/2013/koreasejong/HongSungsik4/10.pdf 
# 추천하는 설명: http://blog.naver.com/PostView.nhn?blogId=parksehoon1971&logNo=220984787036&categoryNo=30&parentCategoryNo=0&viewDate=&currentPage=1&postListTopCurrentPage=1&from=postView
# 데이터프레임으로 카이제곱 검정하기 http://rfriend.tistory.com/137

os <- table(df$operatingSystem, df$isRegister)
model <- table(dt.na$mobileDeviceModel, dt.na$isRegister)

chisq.test(model)

#chisq.test(table(survey$W.Hnd), p=c(.3, .7))
#chisq.test(table(dt$isRegister))
#chisq.test(dt[,-1])
#hisq.test(as.matrix(df[,-1]))

# barplot으로 데이터 분포 파악 
barplot(os, beside = TRUE, legend = TRUE)

# csv exporting 
write.csv(data, file="ga360-data-20180621-2.csv", row.names = TRUE)

#### categorize continuous varialbe with dplyr 
#https://stackoverflow.com/questions/40380112/categorize-continuous-variable-with-dplyr 

set.seed(123)
df <- data.frame(a=rnorm(100))
df$category[df$a < 0.5] <- "low"
df$category[df$a < 0.5 & df$a <0.6] <- "middle"
df$category[df$a > 0.6] <- "high"

library(dplyr)
res <- df %>% mutate(category = cut(a, breaks = c(-Inf, 0.5, 0.6, Inf), labels = c("low", "middle", "high")))

xs = quantile(df$a, c(0,1/3,2/3,1))
xs[1] = xs[1]-.00005
df <-  df %>% mutate(category = cut(a, breaks = xs, labels=c("low","middle","high")))
boxplot(df$a~df$category, col=3:5)
