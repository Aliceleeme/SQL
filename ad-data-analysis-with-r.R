#### data analysis with Bigquery and R ####
# Written by Jihye Lee 
# from 2018-06-22 to 

#### Bigquery data extraction #### 
library(bigrquery)

# project, dataset 이름 정의 
projectName <- "ga360-export" 
datasetName <- "ga_sessions_web_view"

ds <- bq_dataset(projectName, datasetName)

# 실전용; device 데이터 추출 

##sql <- "
  ##SELECT TABLE_DATE, fullVisitorId, browser, operatingSystem, operatingSystemVersion, mobileDeviceBranding, mobileDeviceModel, deviceCategory 
  ##FROM `v_device` 
  ##INNER JOIN ``v_customDimensions_pivot`
  ##WHERE TABLE_DATE BETWEEN @startDate AND @endDate;"

# mobiledevice에서 떼어내서 tabletdivicemodel 다시 만들어야 함
#프로젝트, 데이터셋 지정
projectName <- "ga360-export" # put your projectID here
datasetName <- "ga_sessions_web_view"

ds <- bq_dataset(projectName , datasetName)

# startDate, endDate를 조회 parameter로 지정
sql <- "
select
de.TABLE_DATE,
de.operatingSystem,
de.operatingSystemVersion,
de.browser,
de.browserVersion,
de.screenResolution,
de.deviceCategory,
case when de.deviceCategory in ('mobile','tablet') then 1
else 0 end as isMobile,
de.mobileDeviceBranding,
de.mobileDeviceModel,
case when cd.customDimensions_18 in ('A','B','C') then 1
else 0 end as isRegister,
case when cd.customDimensions_18 in ('A') then 1
else 0 end as isApply
from
`ga_sessions_web_view.v_device` as de
join `ga_sessions_web_view.v_customDimensions_pivot` as cd
on de.sessionId = cd.sessionId
where de.TABLE_DATE between @startDate and @endDate
and cd.TABLE_DATE between @startDate and @endDate
order by de.TABLE_DATE, de.sessionId
"

#조회쿼리를 실행하여 결과를 tb변수에 담는다.
#parameters 속성에 SQL에 지정한 파라미터의 값을 입력해 준다.
tb <- bq_dataset_query(ds,
                       query = sql,
                       parameters = list(startDate = "20180501" , endDate = "20180531"),
                       billing = NULL,
                       quiet = NA)

#쿼리 실행결과를 다운로드하여 result변수에 담는다.
result <- bq_table_download(tb , 
                            page_size = 10000, 
                            start_index = 0, 
                            max_connections = 10, 
                            max_results = 1000000)

head(result)
print(result)

#### Data wrangling #### 
#ref https://rpubs.com/jmhome/R_data_processing
data <- result
dt <- data # data backup

# Exporting the data from directory 
getwd()
dt <- read.csv("ga360-data-20180621-2.csv")

library(readr)
#dt <- read_csv("ga360-data-20180621-2.csv")
dt <- read_csv("ga360-data-may-20180704.csv")
str(data)
head(data)

# data 결측치 전부 제거 (신중함 필요)
sum(is.na(data))
dt <- na.omit(data)

# 데이터 기기 타입에 따라 나누기 
#기기타입 변환 
#MOBILE 
dt$isMobile <-
  ifelse(dt$deviceCategory %in% 'mobile', 1,
  ifelse(dt$deviceCategory %in% 'tablet', 2,
  	0))

#PC 
dt$isPC <- ifelse(dt$deviceCategory %in% 'desktop', 1, 0)

#pc/mobile 변환 
dtPC <- subset(dt, isPC == 1)
dtMobile <- subset(dt, isPC == 0)
dt <- dtMobile #데이터 복사 
dt <- dt[, c(3:14)]

# 데이터 샘플링  
# 샘플링 방법론 참고: http://rfriend.tistory.com/58 
dt <- sample(dt, 1000, replace=F)

#— 2. 그룹수, 70% : 30%
idx <- sample(2, nrow(dt), replace = TRUE, prob = c(0.7, 0.3))

idx <- sample(2, nrow(data), replace = TRUE, prob = c(0.7, 0.3))
train <- data[idx == 1, ]
test <- data[idx == 2, ]

# 불균형을 줄이는 샘플링 
dtmobile$isRegister2 <- dtmobile$isRegister

#https://thebook.io/006723/ch10/06/01/ 
library(caret) 
train <- upSample(subset(dt, select = -isRegister2), dt$isRegister) #업샘플링: 해당 분류에 속하는 데이터가 적은 쪽을 표본으로 더 많이 추출하는 방법
train2 <- downSample(subset(dt, select = -isRegister2), dt$isRegister) #다운샘플링: 데이터가 많은 쪽을 적게 추출하는 방법
　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　 #다운샘플링 하면 5:5로 비율 맞춰져서 데이터 축소 됨 
# downsample한 데이터의 상황 점검 
length(which(train$isRegister==0))
length(which(train$isRegister==1))

length(which(train2$isRegister==0))
length(which(train2$isRegister==1))

hist(train2$isRegister)

#sampling http://mgdjaxo.blogspot.com/2015/06/r-sample-function.html

# data distribution (EDA)
hist(dt$isRegister)
hist(dt$isApply)

summary(dt) #데이터 변수별 값 현황 요약 
var(dt) #분산
sd(dt) #표준편차 

# 특정조건의 변수 삭제/제외하기 
# http://knight76.tistory.com/entry/R-data-table%EC%97%90%EC%84%9C-%ED%8A%B9%EC%A0%95-%EC%A1%B0%EA%B1%B4%EC%9D%98-%EB%8D%B0%EC%9D%B4%ED%84%B0-%EC%A0%9C%EC%99%B8%ED%95%98%EA%B8%B0
#library(data.table)
#dt <- dt[!(dt$operatingSystem  == Java)]
#table 

# ----- 데이터 타입 바꾸기  ----- # 

# factor to numeric 
dt$broswerVersion <- as.numeric(dt$broswerVersion)
dt$operatingSystemVersion <- as.numeric(dt$broswerVersion)

# to factor # http://statkclee.github.io/r-novice-inflammation/01-supp-factors-kr.html
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

dt$isMobile <- as.factor(dt$isMobile)
dt$isPC <- as.factor(dt$isPC)

#종속변수 
dt$isRegister <- as.factor(dt$isRegister)
dt$isApply <- as.factor(dt$isApply)


# categorical variables의 분포 알아보기 (data_type = factor일 때만 사용 가능)
levels(dt$mobileDeviceBranding)
levels(dt$browser)

# 접수와 승인의 갯수 알아보기 
length(which(dt$isRegister==1))  #isregister에서 1의 갯수 (접수)
length(which(dt$isApply==1))  #isapply에서 1의 갯수 (승인)
length(which(dt$isMobile==1)) #ismobile에서 0의 갯수 
length(which(dt$TABLE_DATE=="20180531")) #해당 날짜에 데이터 존재 유무 확인 

#operating system 확인 
length(which(dt$operatingSystem =="Tizen"))
length(which(dt$mobileDeviceBranding=='Point Mobile'))

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
#df$category <- cut(df$a, breaks=c(-Inf, 0.5, 0.6, Inf), labels=c("low","middle","high"))

# ----- 새 컬럼으로 카테고리 데이터로 바꾸기 ----- # 
# 코드 개선이 필요; http://statdb1.uos.ac.kr/computing/r-data-handling.php
#                  https://stackoverflow.com/questions/24459752/can-dplyr-package-be-used-for-conditional-mutating?rq=1

#OS 
dt$operatingSystem2 <- 
  ifelse(dt$operatingSystem %in% 'Android', 1,
  ifelse(dt$operatingSystem %in% 'iOS', 2, 
  ifelse(dt$operatingSystem %in% 'Blackberry', 3, 
  ifelse(dt$operatingSystem %in% 'Chrome OS', 4,
  ifelse(dt$operatingSystem %in% 'Linux', 5,
  ifelse(dt$operatingSystem %in% 'Macintosh', 6,
  ifelse(dt$operatingSystem %in% 'Windows', 7,
  	0)))))))

#Mobile device brand 
dt$mobileDeviceBranding2 <-
  ifelse(dt$mobileDeviceBranding %in% 'Samsung', 1,
  ifelse(dt$mobileDeviceBranding %in% 'Apple', 2, 
  ifelse(dt$mobileDeviceBranding %in% 'LG', 3, 
  ifelse(dt$mobileDeviceBranding %in% 'Google', 4,
  ifelse(dt$mobileDeviceBranding %in% 'Huawei', 5,
  ifelse(dt$mobileDeviceBranding %in% 'KT Tech', 6,
  ifelse(dt$mobileDeviceBranding %in% 'Lava', 7,
  ifelse(dt$mobileDeviceBranding %in% 'Pantech', 8,
  ifelse(dt$mobileDeviceBranding %in% 'Sony', 9,  
  ifelse(dt$mobileDeviceBranding %in% 'Xiaomi', 10,
  ifelse(dt$mobileDeviceBranding %in% 'Windows', 11,
  ifelse(dt$mobileDeviceBranding %in% 'Blackberry', 12,
  ifelse(dt$mobileDeviceBranding %in% 'Microsoft', 12,
         #코드수정 필요 2018-07-10 
         
  ifelse(dt$mobileDeviceBranding %in% 'Alldaymall', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Asus', 16, 
  ifelse(dt$mobileDeviceBranding %in% 'Amazon', 16, 
  ifelse(dt$mobileDeviceBranding %in% 'Alcatel', 16, 
  ifelse(dt$mobileDeviceBranding %in% 'Nokia', 16, #etc
  ifelse(dt$mobileDeviceBranding %in% 'Acer', 16, #etc
  ifelse(dt$mobileDeviceBranding %in% 'Canaima', 16, #etc
  ifelse(dt$mobileDeviceBranding %in% 'Meizu', 16, #etc
  ifelse(dt$mobileDeviceBranding %in% 'TCL', 16,
  ifelse(dt$mobileDeviceBranding %in% 'OPPO', 16,
  ifelse(dt$mobileDeviceBranding %in% 'IPPO', 16,
  ifelse(dt$mobileDeviceBranding %in% 'OnePlus', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Luna', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Wiko', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Vivo', 16,
  ifelse(dt$mobileDeviceBranding %in% 'TG & Company', 16, 
  ifelse(dt$mobileDeviceBranding %in% 'CAT', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Point Mobile', 16,
  ifelse(dt$mobileDeviceBranding %in% 'LeEco', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Sharp', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Sky Devices', 16,
  ifelse(dt$mobileDeviceBranding %in% 'SonyEricsson', 16,
  ifelse(dt$mobileDeviceBranding %in% 'HTC', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Motorola', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Nvidia', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Lava', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Nextbit', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Hannspree', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Chuwi', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Lenovo', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Opera Software', 16,
  	0)))))))))))))))))))))))))))))))))))))))))))))           

#pc = ms, apple, samsung, LG, google, notset 

# pc인 조건과 mobile인 조건을 따로 추출/데이터 분석해서 돌려봐야 하는거 아닌가 싶음

#Browser 
dt$browser2 <-
  ifelse(dt$browser %in% "YaBrowser", 1,  #0 = etc
  ifelse(dt$browser %in% "Amazon Silk", 1,  #0 = etc
  ifelse(dt$browser %in% "Android Runtime", 1,  #0 = etc
  ifelse(dt$browser %in% "Android WebApps", 1,  #0 = etc
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
  ifelse(dt$browser %in% "Android Webview", 4, #2665
  ifelse(dt$browser %in% "Safari", 5, #1074
  ifelse(dt$browser %in% "Internet Explorer", 6, #48
  ifelse(dt$browser %in% "Opera", 7,  #5
  ifelse(dt$browser %in% "Firefox", 8, #3
  ifelse(dt$browser %in% "BlackBerry", 9, #1
  ifelse(dt$browser %in% "Mozilla Compatible Agent", 10, #2
         0)))))))))))))))))))))))

# device model
# 브랜드별 기기 분류; 고급형, 보급형, 등등 
# 2018-07-09 코드 작동 확인 

train2$mobilemodel <- 
  ifelse(train2$mobileDeviceModel %in% 'iPhone', 1,
  ifelse(train2$mobileDeviceModel %in% c('SM-G955N', 'SM-G950N', 'SM-G930S', 'SM-G935S', 'SM-G965N', 'SM-G930L', 'SM-G935L','SM-G935K','SM-G960N', 'SM-G920S', 'SM-G920K', 'SM-G925K', 'SM-G920L', 'SM-G930K', 'SM-G925L', 'SM-G928S'), 2,
  ifelse(train2$mobileDeviceModel %in% c('SM-G610L', 'SM-G610S', 'SM-G925S', 'SM-G610K', 'SM-G920A', 'SM-G928L', 'SM-G720N0', 'SM-G906S','SM-G906K', 'SM-G928K', 'SM-G610K/KKU1APL1', 'SM-G600S', 'SHV-E330S', 'SHV-E250S'), 2,
  ifelse(train2$mobileDeviceModel %in% c('SM-N920S','SM-N950N', 'SM-N935K', 'SM-N935S', 'SM-N920K', 'SM-N920L', 'SM-N910S','SM-N916L', 'SM-N910L', 'SM-N900S', 'SM-N916S', 'M-N910K', 'M-N915S', 'SM-N935L', 'SM-N916K', 'SM-N900L'), 2, 
  ifelse(train2$mobileDeviceModel %in% c('SM-A530N', 'SM-A720S', 'SM-A710L', 'SM-A810S', 'SM-A710K', 'SM-A520F', 'SM-A520L', 'SM-A800S', 'SM-A520S', 'SM-A520K', 'SM-A500L', 'SM-A710S', 'SM-A310N0', 'SM-A510S', 'SM-A700L'), 3, 
  ifelse(train2$mobileDeviceModel %in% c('SM-J530S','SM-J730K', 'SM-J710K', 'SM-J727S', 'SM-J500N0','SM-J330', 'SM-J510H', 'SM-J510L', 'SM-J510S', 'SM-J510K', 'SM-J530L'), 4,
  ifelse(train2$mobileDeviceModel %in% c('LGM-V300L', 'F800S', 'F800K', 'F600L', 'F700S', 'F700K', 'F700L', 'LG-F700L', 'H830', 'M-G600K', 'LS991', 'LGM-X600L', 'LGM-G600L', 'LGM-G600S', 'V300L', 'LG-F800L', 'F600S ', 'LG-F600L', 'LGM-X600S', 'LGM-K120L', 'LGM-X320L'), 5, 
  ifelse(train2$mobileDeviceModel %in% c('LGM-X600S', 'LGM-K120L', 'LGM-X320L'), 6, 
  ifelse(train2$mobileDeviceModel %in% c('IM-100S', 'IM-A890K'), 7,  
  ifelse(train2$mobileDeviceModel %in% 'Windows RT Tablet', 8, 9
  ))))))))))

# ----------------------------------------------------------------------- # 
# ----------------------------------------------------------------------- # 

dt$mobileDeviceModel2 <- 
  ifelse(dt$mobileDeviceModel %in% 'iPhone', 1, #아이폰 (고급형)
  ifelse(dt$mobileDeviceModel %in% 'SM-G955N', 2, #갤럭시 시리즈 시작 (고급형)
  ifelse(dt$mobileDeviceModel %in% 'SM-G950N', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G930S', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-G935S', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-G965N', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G930L', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G935L', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G935K', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G960N', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G920S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G920K', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-G925K', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G920L', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G930K', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G925L', 2,  
  ifelse(dt$mobileDeviceModel %in% 'SM-G928S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G610L', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G610S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G925S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G610K', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G920A', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G928L', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G720N0', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-G906S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G906K', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G928K', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G610K/KKU1APL1', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-G600S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SHV-E330S', 2,  #갤럭시S4 
  ifelse(dt$mobileDeviceModel %in% 'SHV-E250S', 2, #갤럭시 시리즈 끝 (고급형)
  ifelse(dt$mobileDeviceModel %in% 'SM-N950N/KSU1AQI2', 2,  #갤럭시 노트 시리즈 시작 (고급형)
  ifelse(dt$mobileDeviceModel %in% 'SM-N920S', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-N950N', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-N935K', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-N935S', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-N920K', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-N920L', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-N910S', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-N916L', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-N910L', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-N900S', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-N916S', 2, 
  ifelse(dt$mobileDeviceModel %in% 'M-N910K', 2,  #갤럭시노트4 
  ifelse(dt$mobileDeviceModel %in% 'M-N915S', 2,  #갤럭시노트 엣지 
  ifelse(dt$mobileDeviceModel %in% 'SM-N935L', 2,  
  ifelse(dt$mobileDeviceModel %in% 'SM-N916K', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-N900L', 2, #갤럭시 노트 시리즈 (고급형) 끝 
  ifelse(dt$mobileDeviceModel %in% 'SM-A530N', 2, #갤럭시 A시리즈 시작 (중급형)
  ifelse(dt$mobileDeviceModel %in% 'SM-A720S', 2, 
  ifelse(dt$mobileDeviceModel %in% 'SM-A710L', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A810S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A710K', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A520F', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A520L', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A800S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A520S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A520K', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A500L', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A710S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A310N0', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A510S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-A700L', 2,  #갤럭시 A시리즈 (중급형) 끝 
  ifelse(dt$mobileDeviceModel %in% 'SM-J530S', 2,  #갤럭시 j시리즈 (보급형) 시작 
  ifelse(dt$mobileDeviceModel %in% 'SM-J730K', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-J710K', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-J727S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-J500N0', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-J330', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-J510H', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-J510L', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-J510S', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-J510K', 2,
  ifelse(dt$mobileDeviceModel %in% 'SM-J530L', 2, #갤럭시 j시리즈 (보급형)  끝 

  ifelse(dt$mobileDeviceModel %in% 'F700S', 3, #엘지 g5  #LG G시리즈 시작 (고급형)
  ifelse(dt$mobileDeviceModel %in% 'F700K', 3, #엘지 g5 
  ifelse(dt$mobileDeviceModel %in% 'F700L', 3,#엘지 g5 
  ifelse(dt$mobileDeviceModel %in% 'LG-F700L', 3,  #엘지 g5 
  ifelse(dt$mobileDeviceModel %in% 'H830', 3,  #엘지 g5 usa 
  ifelse(dt$mobileDeviceModel %in% 'M-G600K', 3, #엘지 g6 (고급형)
  ifelse(dt$mobileDeviceModel %in% 'LS991', 3, #엘지 USA 한정 
  ifelse(dt$mobileDeviceModel %in% 'LGM-X600L', 3, #엘지 g6 
  ifelse(dt$mobileDeviceModel %in% 'LGM-G600L', 3, #엘지 g6 
  ifelse(dt$mobileDeviceModel %in% 'LGM-G600S', 3, #엘지 g6 #LG G시리즈 끝 (고급형)
  ifelse(dt$mobileDeviceModel %in% 'LGM-V300L', 3, #엘지 v30 thinq #LG V시리즈 시작 (고급형)
  ifelse(dt$mobileDeviceModel %in% 'F800S', 3, #엘지 V20 
  ifelse(dt$mobileDeviceModel %in% 'F800K', 3,#엘지 V20 
  ifelse(dt$mobileDeviceModel %in% 'F600L', 3, #엘지 V10 
  ifelse(dt$mobileDeviceModel %in% 'V300L', 3, #엘지 v30 
  ifelse(dt$mobileDeviceModel %in% 'LG-F800L', 3,  #엘지 v20
  ifelse(dt$mobileDeviceModel %in% 'F600S ', 3,  #엘지 V10 
  ifelse(dt$mobileDeviceModel %in% 'LG-F600L', 3,  #엘지 V10 #LG V시리즈 끝 (고급형)
  ifelse(dt$mobileDeviceModel %in% 'LGM-X600S', 3, #엘지 q6 (중급형) #LG Q시리즈 == 중급
  ifelse(dt$mobileDeviceModel %in% 'LGM-K120L', 3, #엘지 x300 (보급형) #LG X시리즈 == 보급 
  ifelse(dt$mobileDeviceModel %in% 'LGM-X320L', 3, #엘지 x500 (보급형)
  ifelse(dt$mobileDeviceModel %in% 'IM-100S', 4, #스카이 im100 (보급형)
  ifelse(dt$mobileDeviceModel %in% 'IM-A890K', 4, #팬텍 베가 시크릿노트 (고급형)
  ifelse(dt$mobileDeviceModel %in% 'Windows RT Tablet', 5, #surface (마소) -> ??? 
    0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))


# ----------------------------------------------------------------------- # 
# ----------------------------------------------------------------------- # 

# 특정 열 삭제 혹은 추출 

dt2 <-dt[, c(1:5)] #열 
dt2 <- dt[c(3:10), ] #행

test <- train2[, c(2, 4, 6, 7, 10, 13:17)]

# ----------------------------------------------------------------------- # 

#독립성검정
attach(train2)
detach(train2)

#이변량분할표 contingency table 
#http://rfriend.tistory.com/tag/gmodels%20package

table <- xtabs(~operatingSystem2+isRegister, data=test) # https://m.blog.naver.com/PostView.nhn?blogId=liberty264&logNo=220985283476&proxyReferer=https%3A%2F%2Fwww.google.co.kr%2F

table <- xtabs(~mobileDeviceBranding2+isRegister, data=test) 
table <- xtabs(~browser2+isRegister, data=test) 
table <- xtabs(~mobilemodel+isRegister, data=test) 

table

# 모자이크 그래프 
mosaic(table, 
        gp=gpar(fill=c("red", "blue")), 
        direction="v", 
        main="Mosaic plot")
 
chisq.test(table) 
#귀무가설: 성별과 운동량은 관계가 있다 
#p<0.05 - 귀무가설 성립 

# ----------------------------------------------------------------------- # 

# factor to numeric 
train2$operatingSystem2 <- as.numeric(train2$operatingSystem2)
train2$mobileDeviceBranding2 <- as.numeric(train2$mobileDeviceBranding2)
train2$browser2 <- as.numeric(train2$browser2)
test$devicecategory <- as.numeric(test$devicecategory)

sum(is.na(test))
test <- na.omit(test)

# ----------------------------------------------------------------------- # 

# 1차 chi^2 분석 후 데이터 재정제 

#if문 작성 방법 
#모바일 기종

train2$mobilemodel <- 
  ifelse(train2$mobilemodel %in% 4, 3,
  ifelse(train2$mobilemodel %in% 7, 9, 
  ifelse(train2$mobilemodel %in% 8, 9, 
  ifelse(train2$mobilemodel %in% 'Chrome OS', 4,
  ifelse(train2$mobilemodel %in% 'Linux', 5,
  ifelse(train2$mobilemodel %in% 'Macintosh', 6,
  ifelse(train2$mobilemodel %in% 'Windows', 7,
 0)))))))

            isRegister
mobilemodel    0    1
          1  396  844
          2 2340 2038
          3  204  198 #삼성 a시리즈 
          4  136   93 #삼성 j시리즈 
          5  253  293
          7    1   12 #스카이 
          8    6    4 #서피스 
          9  519  373 #기타 

# 7,8 삭제 

#브라우저 종료 
train2$browser2 <- 
  ifelse(train2$browser2 %in% 'Android', 1,
  ifelse(train2$browser2 %in% 'iOS', 2, 
  ifelse(train2$browser2 %in% 'Blackberry', 3, 
  ifelse(train2$browser2 %in% 'Chrome OS', 4,
  ifelse(train2$browser2 %in% 'Linux', 5,
  ifelse(train2$browser2 %in% 'Macintosh', 6,
  ifelse(train2$browser2 %in% 'Windows', 7,
 0)))))))

        isRegister
browser2    0    1
       1  224  585
       3 1911 1578
       4  899 1053
       5  608  368
       6  203  267
       7    9    4  #opera 
       9    1    0  #블랙베리 
# 7,9 삭제 


#모바일 기기 브랜드  
#디바이스 브랜드랑 기종이랑 안맞아? 
train2$mobileDeviceBranding2 <- 
  ifelse(train2$mobileDeviceBranding2 %in% 'Android', 1,
  ifelse(train2$mobileDeviceBranding2 %in% 'iOS', 2, 
  ifelse(train2$mobileDeviceBranding2 %in% 'Blackberry', 3, 
  ifelse(train2$mobileDeviceBranding2 %in% 'Chrome OS', 4,
  ifelse(train2$mobileDeviceBranding2 %in% 'Linux', 5,
  ifelse(train2$mobileDeviceBranding2 %in% 'Macintosh', 6,
  ifelse(train2$mobileDeviceBranding2 %in% 'Windows', 7,
    0)))))))

                      isRegister
mobileDeviceBranding2    0    1
                   1    42   26 #? 삼성 
                   2  2993 2577 
                   3   414  846
                   4   364  351
                   5     2    5 #? 화웨이 
                   6     7   13 #? kt tech
                   9    13   21 #? 소니
                   10    3    6 #? 샤오미
                   11    3    3 #? 윈도우
                   12    6    4 #? 화웨이 
                   13    8    3 #? 블랙베리+마소 
## 10의 자리수인 애들 다 묶어서 새로운 분류로 만들 것 


# ----------------------------------------------------------------------- # 

#https://stats.stackexchange.com/questions/81483/warning-in-r-chi-squared-approximation-may-be-incorrect
#카이제곱검정 http://rfriend.tistory.com/112

library(gmodels) #Tools for Model Fitting #http://rfriend.tistory.com/120
library(vcd) 

attach(train2) 
#detach(train2)

CrossTable(deviceCategory, isRegister, # crosstable = 교차분석 http://dbrang.tistory.com/1067 
            expected = TRUE, # expected frequency
            chisq = TRUE) # chisq-test 

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

# ----------------------------------------------------------------------- # 

# csv exporting 
write.csv(data, file="ga360-data-20180704-2.csv", row.names = TRUE) 
