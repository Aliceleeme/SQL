

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
                            page_size = 500 , 
                            start_index = 1 , 
                            max_connections = 5, 
                            max_results = 500000) #max_result 범위 늘려서 결과값 추출 범위 늘리기 

print(result)

#### Data wrangling #### 
#ref https://rpubs.com/jmhome/R_data_processing

data <- result

# variables
str(data)
head(data)

# data backup
dt <- data

#Call the data from directory 
library(readr)
dt <- read_csv("ga360-data-20180621-2.csv")

# data 결측치 전부 제거 (신중함 필요)
sum(is.na(dt))
dt.na <- na.omit(dt)

# screen size; plus size, general size and etc
y <- subset(dt, screenResolution = '1024x768')

# operatingSystem이 NA인 데이터만 출력
library(dplyr)
dt <- dt %>% filter(is.na(operatingSystem))   # operatingSystem이 NA인 데이터만 출력
df_nomiss <- df %>% filter(!is.na(operatingSystem))   # operatingSystem 결측치 제거

# Exploring data distribution (EDA)
hist(dt$isRegister)
hist(dt$isApply)

# 특정조건의 변수 삭제/제외하기 (working on it)
# http://knight76.tistory.com/entry/R-data-table%EC%97%90%EC%84%9C-%ED%8A%B9%EC%A0%95-%EC%A1%B0%EA%B1%B4%EC%9D%98-%EB%8D%B0%EC%9D%B4%ED%84%B0-%EC%A0%9C%EC%99%B8%ED%95%98%EA%B8%B0
library(data.table)
table <- table[!(table$variable == score)]
table 

# ----- 데이터 타입 바꾸기  ----- # 
# factor to numeric 
dt$broswerVersion <- as.numeric(dt$broswerVersion)
dt$operatingSystemVersion <- as.numeric(dt$broswerVersion)

# to factor 
dt$operatingSystemVersion <- as.factor(dt$operatingSystemVersion)

dt$operatingSystem <- as.factor(dt$operatingSystem)
dt$browser <- as.factor(dt$browser)

dt$deviceCategory <- as.factor(dt$deviceCategory)
dt$mobileDeviceBranding <- as.factor(dt$mobileDeviceBranding)
dt$mobileDeviceModel <- as.factor(dt$mobileDeviceModel)

dt$deviceCategory2 <- as.factor(dt$deviceCategory2)
dt$mobileDeviceBranding2 <- as.factor(dt$mobileDeviceBranding2)
dt$operatingSystem2 <- as.factor(dt$operatingSystem2)
dt$deviceCategory2 <- as.factor(dt$deviceCategory2)

#종속변수 
dt$isMobile <- as.factor(dt$isMobile)
dt$isRegister <- as.factor(dt$isRegister)
dt$isApply <- as.factor(dt$isApply)

# 컬럼 삭제
library(dplyr)
dt <- 
  dt %>%
   select(operatingSystem, operatingSystemVersion, browser, browserVersion, deviceCategory, 
          isMobile, mobileDeviceBranding, mobileDeviceModel, isRegister, isApply, 
          deviceCategory2, operatingSystem2, mobileDeviceBranding2, operatingSystemVersion2)

# categorical variables의 분포 알아보기 (data_type = factor일 때만 사용 가능)
levels(dt$mobileDeviceBranding)
levels(dt$mobileDeviceBranding)

levels(dt$browser)
levels(dt$operatingSystem)

# 접수와 승인의 갯수 알아보기 

#isregister에서 1의 갯수 (접수)
length(which(dt$isRegister==1)) 
length(which(dt$isRegister==0))

#isapply에서 1의 갯수 (승인)
length(which(dt$isApply==1)) 
length(which(dt$isApply==0))

#ismobile에서 0의 갯수 
length(which(dt$isMobile==0)) 
length(which(dt$isMobile==1))

#해당 날짜에 데이터 존재 유무 확인 
length(which(dt$TABLE_DATE=="20180531"))
length(which(dt$TABLE_DATE=="20180515")) 

#브라우저 데이터 분포 파악 
length(which(dt$browser=="Android Runtime")) #0
length(which(dt$browser=="Android WebApps")) #0
length(which(dt$browser=="Android Webview")) #2665
length(which(dt$browser=="BrowserNG")) #0
length(which(dt$browser=="BlackBerry")) #1
length(which(dt$browser=="Chrome")) #7439
length(which(dt$browser=="Coc Coc")) #0
length(which(dt$browser=="Edge")) #0
length(which(dt$browser=="Firefox")) #3
length(which(dt$browser=="IE with Chrome frame")) #0
length(which(dt$browser=="Internet Explorer")) #48
length(which(dt$browser=="Java")) #0
length(which(dt$browser=="Mozilla Compatible Agent")) #2
length(which(dt$browser=="NokiaC7-00")) #0
length(which(dt$browser=="Opera")) #5
length(which(dt$browser=="Puffin")) #0
length(which(dt$browser=="Safari")) #1074
length(which(dt$browser=="Sarari (in-app)")) #0
length(which(dt$browser=="Samsung Internet")) #3460
length(which(dt$browser=="UC Browser")) #0
length(which(dt$browser=="YaBrowser")) #0


#카테고리 데이터 레이블링하기 (Sample code)
df$category <- cut(df$a, breaks=c(-Inf, 0.5, 0.6, Inf), labels=c("low","middle","high"))


# ----- 새 컬럼으로 카테고리 데이터로 바꾸기 ----- # 

dt$operatingSystem2 <- 
  ifelse(dt$operatingSystem %in% 'Android', 1,
  ifelse(dt$operatingSystem %in% 'iOS', 2, 
  ifelse(dt$operatingSystem %in% 'Blackberry', 3, 
  ifelse(dt$operatingSystem %in% 'Chrome OS', 4,
  ifelse(dt$operatingSystem %in% 'Linux', 5,
  ifelse(dt$operatingSystem %in% 'Macintosh', 6,
  ifelse(dt$operatingSystem %in% 'Windows', 7,
  	0)))))))

dt$mobileDeviceBranding2 <-
  ifelse(dt$mobileDeviceBranding %in% 'Alcatel', 1,
  ifelse(dt$mobileDeviceBranding %in% 'Apple', 2, 
  ifelse(dt$mobileDeviceBranding %in% 'Blackberry', 3, 
  ifelse(dt$mobileDeviceBranding %in% 'Google', 4,
  ifelse(dt$mobileDeviceBranding %in% 'Huawei', 5,
  ifelse(dt$mobileDeviceBranding %in% 'KT Tech', 6,
  ifelse(dt$mobileDeviceBranding %in% 'Lava', 7,
  ifelse(dt$mobileDeviceBranding %in% 'OPPO', 8,
  ifelse(dt$mobileDeviceBranding %in% 'Pantech', 9,
  ifelse(dt$mobileDeviceBranding %in% 'Samsung', 10, 
  ifelse(dt$mobileDeviceBranding %in% 'Sony', 11, 
  ifelse(dt$mobileDeviceBranding %in% 'TCL', 12,
  ifelse(dt$mobileDeviceBranding %in% 'TG & Company', 13,
  ifelse(dt$mobileDeviceBranding %in% 'Xiaomi', 14,
  ifelse(dt$mobileDeviceBranding %in% 'Windows', 15,
  	0)))))))))))))))

dt$deviceCategory2 <-
  ifelse(dt$deviceCategory %in% 'mobile', 1,
  ifelse(dt$deviceCategory %in% 'desktop', 2, 
  ifelse(dt$deviceCategory %in% 'tablet', 3,
  	0)))

dt$browser2 <-
  ifelse(dt$browser %in% "Android Runtime", 1,  #0 = etc
  ifelse(dt$browser %in%"Android WebApps", 1,  #0 = etc
  ifelse(dt$browser %in% "Coc Coc", 1, #0 = etc
  ifelse(dt$browser %in% "Edge", 1,  #0 = etc
  ifelse(dt$browser %in% "BrowserNG", 1, #0 = etc
  ifelse(dt$browser %in% "IE with Chrome frame", 1, #0 = etc
  ifelse(dt$browser %in% "NokiaC7-00", 1, #0 = etc
  ifelse(dt$browser %in% "Puffin", 1, #0 = etc
  ifelse(dt$browser %in% "Sarari (in-app)", 1, #0 = etc
  ifelse(dt$browser %in% "UC Browser", 1, #0 = etc
  ifelse(dt$browser %in% "YaBrowser", 1, #0 = etc
  ifelse(dt$browser %in% "Java", 1, #0 #java는 그냥 지우는게 좋을 것 같은데 개발자 키트라서 
  ifelse(dt$browser %in% "Chrome", 2, #7439
  ifelse(dt$browser %in% "Samsung Internet", 3, #3460
  ifelse(dt$browse %in% "Android Webview", 4, #2665
  ifelse(dt$browser %in% "Safari", 5, #1074
  ifelse(dt$browser %in% "Internet Explorer", 6, #48
  ifelse(dt$browser %in% "Opera", 7,  #5
  ifelse(dt$browser %in% "Firefox", 8, #3
  ifelse(dt$browser %in% "BlackBerry", 9, #1
  ifelse(dt$browser %in% "Mozilla Compatible Agent", 10, #2
         0))))))))))))))))))))))

  
dt$browser2 <-
  ifelse(dt$browser %in% "Android Runtime", 1,  #0 = etc
  ifelse(dt$browser %in%"Android WebApps", 1,  #0 = etc
  ifelse(dt$browser %in% "Coc Coc", 1, #0 = etc
  ifelse(dt$browser %in% "Edge", 1,  #0 = etc
  ifelse(dt$browser %in% "BrowserNG", 1, #0 = etc
  ifelse(dt$browser %in% "IE with Chrome frame", 1, #0 = etc
  ifelse(dt$browser %in% "NokiaC7-00", 1, #0 = etc
  ifelse(dt$browser %in% "Puffin", 1, #0 = etc
  ifelse(dt$browser %in% "Sarari (in-app)", 1, #0 = etc
  ifelse(dt$browser %in% "UC Browser", 1, #0 = etc
  ifelse(dt$browser %in% "YaBrowser", 1, #0 = etc
  ifelse(dt$browser %in% "Java", 1, #0 #java는 그냥 지우는게 좋을 것 같은데 개발자 키트라서 
  ifelse(dt$browser %in% "Chrome", 2, #7439
  ifelse(dt$browser %in% "Samsung Internet", 3, #3460
  ifelse(dt$browse %in% "Android Webview", 4, #2665
  ifelse(dt$browser %in% "Safari", 5, #1074
  ifelse(dt$browser %in% "Internet Explorer", 6, #48
  ifelse(dt$browser %in% "Opera", 7,  #5
  ifelse(dt$browser %in% "Firefox", 8, #3
  ifelse(dt$browser %in% "BlackBerry", 9, #1
  ifelse(dt$browser %in% "Mozilla Compatible Agent", 10, #2
         0))))))))))))))))))))))


# ----------------------------------------------------------------------- # 

#https://stats.stackexchange.com/questions/81483/warning-in-r-chi-squared-approximation-may-be-incorrect
#카이제곱검정 http://rfriend.tistory.com/112

library(gmodels)
library(vcd)

attach(dt) #factor, chr로 정제된 데이터 넣기 
CrossTable(mobileDeviceBranding, isRegister, # crosstable = 교차분석 http://dbrang.tistory.com/1067 
            expected = TRUE, # expected frequency
            chisq = TRUE) # chisq-test 
detach(dt)

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

