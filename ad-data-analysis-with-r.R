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

ds <- bq_dataset(projectName, datasetName)

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
result <- bq_table_download(tb, 
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


# ----- 데이터 타입 바꾸기  ----- # 

# factor to numeric 
dt$broswerVersion <- as.numeric(dt$broswerVersion)
dt$operatingSystem <- as.factor(dt$operatingSystem) #factor로 바꾸면 summary(data)에서 데이터 분포를 파악할 때 각 변수 내에 분포하는 카테고리 종류가 몇개씩 들어있는지 알 수 있음 

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


# 카테고리 데이터의 분포 알아보기 (data_type = factor일 때만 사용 가능)
levels(dt$mobileDeviceBranding)
levels(dt$browser)

# 접수와 승인의 갯수 알아보기 
length(which(dt$isRegister==1))  #isregister에서 1의 갯수 (접수)
length(which(dt$isApply==1))  #isapply에서 1의 갯수 (승인)

#해당 날짜에 데이터 존재 유무 확인 
length(which(dt$TABLE_DATE=="20180531")) #

#operating system 확인 
length(which(dt$operatingSystem =="Blackberry"))

#브라우저 데이터 분포 파악 
length(which(dt$browser=="Android Webview")) #2665


#카테고리 데이터 레이블링하기 (Sample code)

# 모바일 OS 시스템 
dt$operatingSystem2 <- 
  ifelse(dt$operatingSystem %in% 'Android', 1,
  ifelse(dt$operatingSystem %in% 'iOS', 2, 
  ifelse(dt$operatingSystem %in% 'Blackberry', 3, 
  ifelse(dt$operatingSystem %in% 'Chrome OS', 4,
  ifelse(dt$operatingSystem %in% 'Linux', 5,
  ifelse(dt$operatingSystem %in% 'Macintosh', 6,
  ifelse(dt$operatingSystem %in% 'Windows', 7,
  	0)))))))

# 모바일 기기 브랜드 
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
  ifelse(dt$mobileDeviceBranding %in% 'Blackberry', 16, # 16 = 기타 브랜드 혹은 원본 데이터에서 분포가 낮은 브랜드들 
  ifelse(dt$mobileDeviceBranding %in% 'Microsoft', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Alldaymall', 16,
  ifelse(dt$mobileDeviceBranding %in% 'Asus', 16, 
  ifelse(dt$mobileDeviceBranding %in% 'Amazon', 16, 
  ifelse(dt$mobileDeviceBranding %in% 'Alcatel', 16, 
  ifelse(dt$mobileDeviceBranding %in% 'Nokia', 16, 
  ifelse(dt$mobileDeviceBranding %in% 'Acer', 16, 
  ifelse(dt$mobileDeviceBranding %in% 'Canaima', 16, 
  ifelse(dt$mobileDeviceBranding %in% 'Meizu', 16, 
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

# 모바일 브라우저 
dt$browser2 <-
  ifelse(dt$browser %in% "YaBrowser", 1,  #1 = 기타 etc
  ifelse(dt$browser %in% "Amazon Silk", 1,  
  ifelse(dt$browser %in% "Coc Coc", 1, 
  ifelse(dt$browser %in% "Edge", 1,  
  ifelse(dt$browser %in% "BrowserNG", 1,
  ifelse(dt$browser %in% "IE with Chrome frame", 1, 
  ifelse(dt$browser %in% "NokiaC7-00", 1, 
  ifelse(dt$browser %in% "Puffin", 1, 
  ifelse(dt$browser %in% "Sarari (in-app)", 1, 
  ifelse(dt$browser %in% "UC Browser", 1, 
  ifelse(dt$browser %in% "YaBrowser", 1,
  ifelse(dt$browser %in% "Java", 1, 
  ifelse(dt$browser %in% "Chrome", 2, 
  ifelse(dt$browser %in% "Samsung Internet", 3,
  ifelse(dt$browser %in% "Android Webview", 4, 
  ifelse(dt$browser %in% "Safari", 5, 
  ifelse(dt$browser %in% "Safari(in-app)", 5, 
  ifelse(dt$browser %in% "Internet Explorer", 6, 
  ifelse(dt$browser %in% "Opera", 7,  
  ifelse(dt$browser %in% "Firefox", 8, 
  ifelse(dt$browser %in% "BlackBerry", 9, 
  ifelse(dt$browser %in% "Mozilla Compatible Agent", 10,
  ifelse(dt$browser %in% "Android Runtime", 11, #11 = android 계열의 브라우저들  
  ifelse(dt$browser %in% "Android WebApps", 11,  
  ifelse(dt$browser %in% "Android Browser", 11,  
         0)))))))))))))))))))))))))

# device model
# 브랜드별 기기 분류; 고급형, 보급형, 등등 

train2$mobilemodel <- 
  ifelse(train2$mobileDeviceModel %in% 'iPhone', 1,
  ifelse(train2$mobileDeviceModel %in% c('SM-G955N', 'SM-G950N', 'SM-G930S', 'SM-G935S', 'SM-G965N', 'SM-G930L', 'SM-G935L','SM-G935K','SM-G960N', 'SM-G920S', 'SM-G920K', 'SM-G925K', 'SM-G920L', 'SM-G930K', 'SM-G925L', 'SM-G928S'), 2,
  ifelse(train2$mobileDeviceModel %in% c('SM-G610L', 'SM-G610S', 'SM-G925S', 'SM-G610K', 'SM-G920A', 'SM-G928L', 'SM-G720N0', 'SM-G906S','SM-G906K', 'SM-G928K', 'SM-G610K/KKU1APL1', 'SM-G600S', 'SHV-E330S', 'SHV-E250S'), 2,
  ifelse(train2$mobileDeviceModel %in% c('SM-N920S','SM-N950N', 'SM-N935K', 'SM-N935S', 'SM-N920K', 'SM-N920L', 'SM-N910S','SM-N916L', 'SM-N910L', 'SM-N900S', 'SM-N916S', 'M-N910K', 'M-N915S', 'SM-N935L', 'SM-N916K', 'SM-N900L'), 2, 
  ifelse(train2$mobileDeviceModel %in% c('SM-A530N', 'SM-A720S', 'SM-A710L', 'SM-A810S', 'SM-A710K', 'SM-A520F', 'SM-A520L', 'SM-A800S', 'SM-A520S', 'SM-A520K', 'SM-A500L', 'SM-A710S', 'SM-A310N0', 'SM-A510S', 'SM-A700L'), 3, 
  ifelse(train2$mobileDeviceModel %in% c('SM-J530S','SM-J730K', 'SM-J710K', 'SM-J727S', 'SM-J500N0','SM-J330', 'SM-J510H', 'SM-J510L', 'SM-J510S', 'SM-J510K', 'SM-J530L'), 4,
  ifelse(train2$mobileDeviceModel %in% c('LGM-V300L', 'F800S', 'F800K', 'F600L', 'F700S', 'F700K', 'F700L', 'LG-F700L', 'H830', 'M-G600K', 'LS991', 'LGM-G600L', 'LGM-G600S', 'V300L', 'LG-F800L', 'F600S ', 'LG-F600L', 'LGM-K120L'), 5, 
  ifelse(train2$mobileDeviceModel %in% c('LGM-X600S', 'LGM-K120L', 'LGM-X320L', 'LGM-X600K', 'LGM-X600L'), 6, 
  ifelse(train2$mobileDeviceModel %in% c('IM-100S', 'IM-A890K'), 7,  
  ifelse(train2$mobileDeviceModel %in% 'Windows RT Tablet', 8, 9
  ))))))))))

# ----------------------------------------------------------------------- # 

# 특정 열 삭제 혹은 추출 

dt2 <-dt[, c(1:5)] #열 
dt2 <- dt[c(3:10), ] #행
test <- train2[, c(2, 4, 6, 7, 10, 13:17)]

# ----------------------------------------------------------------------- # 

#독립성검정
#이변량분할표 contingency table 
# 설명: http://rfriend.tistory.com/tag/gmodels%20package
# 설명 2: https://m.blog.naver.com/PostView.nhn?blogId=liberty264&logNo=220985283476&proxyReferer=https%3A%2F%2Fwww.google.co.kr%2F

table1 <- xtabs(~operatingSystem2+isRegister, data=train2) 
table2 <- xtabs(~mobileDeviceBranding2+isRegister, data=train2) 
table3 <- xtabs(~browser2+isRegister, data=train2) 
table4 <- xtabs(~mobilemodel+isRegister, data=train2) 

table1
table2
table3
table4
 
chisq.test(table1)
chisq.test(table2)
chisq.test(table3)
chisq.test(table4)

# ----------------------------------------------------------------------- # 

# 데이터 카테고리 범주 재설정 
#모바일 기종
dt$mobilemodel <- 
  ifelse(dt$mobilemodel %in% 1, 1,
  ifelse(dt$mobilemodel %in% 2, 2, 
  ifelse(dt$mobilemodel %in% 3, 3, #갤럭시 a, j 결합 (중급+보급형)
  ifelse(dt$mobilemodel %in% 4, 3,
  ifelse(dt$mobilemodel %in% 4, 4,
  ifelse(dt$mobilemodel %in% 5, 5,
  ifelse(dt$mobilemodel %in% 7, 9, # 7, 8 기타 기종으로 결합 
  ifelse(dt$mobilemodel %in% 8, 9,
 0))))))))

#브라우저 종류 
dt$browser2 <- 
  ifelse(dt$browser2 %in% 1, 0,
  ifelse(dt$browser2 %in% 2, 2, #chrome  
  ifelse(dt$browser2 %in% 3, 3, #samsung 
  ifelse(dt$browser2 %in% 4, 4, 
  ifelse(dt$browser2 %in% 5, 5,
  ifelse(dt$browser2 %in% 6, 6,
 0)))))) # 블랙베리, 오페라 결합 없이 0으로 보내버림 


#모바일 기기 브랜드  
dt$mobileDeviceBranding2 <- 
  ifelse(dt$mobileDeviceBranding2 %in% 1, 1, #삼성
  ifelse(dt$mobileDeviceBranding2 %in% 2, 2, 
  ifelse(dt$mobileDeviceBranding2 %in% 3, 3, 
  ifelse(dt$mobileDeviceBranding2 %in% 4, 4,
  ifelse(dt$mobileDeviceBranding2 %in% 5, 5,
   0)))))
    
    

# ----------------------------------------------------------------------- # 

#카이제곱검정 
# 설명 참고: http://rfriend.tistory.com/112

library(gmodels) 
library(vcd) 

attach(train2) 
#detach(train2)

CrossTable(deviceCategory, isRegister, # crosstable = 교차분석
            expected = TRUE, # expected frequency
            chisq = TRUE) # chisq-test 

os <- table(df$operatingSystem, df$isRegister)
model <- table(dt.na$mobileDeviceModel, dt.na$isRegister)

chisq.test(model)


# ----------------------------------------------------------------------- # 

# csv exporting 
write.csv(data, file="ga360-data-20180704-2.csv", row.names = TRUE) 
