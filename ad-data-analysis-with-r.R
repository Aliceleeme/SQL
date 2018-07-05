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
dt <- dtMobile 
dt <- dt[, c(3:14)]

# data distribution (EDA)
hist(dt$isRegister)
hist(dt$isApply)

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

# 컬럼 삭제
library(dplyr)
dt <- 
  dt %>%
   select(operatingSystem, operatingSystemVersion, browser, browserVersion, deviceCategory, 
          isMobile, mobileDeviceBranding, mobileDeviceModel, isRegister, isApply, 
          deviceCategory2, operatingSystem2, mobileDeviceBranding2, operatingSystemVersion2)

# categorical variables의 분포 알아보기 (data_type = factor일 때만 사용 가능)
levels(dt$mobileDeviceBranding)
levels(dt$browser)

# 접수와 승인의 갯수 알아보기 

#isregister에서 1의 갯수 (접수)
length(which(dt$isRegister==1)) 

#isapply에서 1의 갯수 (승인)
length(which(dt$isApply==1)) 

#ismobile에서 0의 갯수 
length(which(dt$isMobile==1))

#해당 날짜에 데이터 존재 유무 확인 
length(which(dt$TABLE_DATE=="20180531"))

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
  ifelse(dt$mobileDeviceBranding %in% 'Huawei', 12,
  ifelse(dt$mobileDeviceBranding %in% 'Blackberry', 13,
  ifelse(dt$mobileDeviceBranding %in% 'Microsoft', 13,
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

# 특정 열 삭제 혹은 추출 
dt2 <-dt[, c(1:5)] #열 
dt2 <- dt[c(3:10), ] #행


# ----------------------------------------------------------------------- # 

#이변량분할표 contingency table 
#http://rfriend.tistory.com/tag/gmodels%20package

dt_table_3 <- with(dt, table(mobileDeviceBranding, isRegister))

mosaic(dt_table_3, 
        gp=gpar(fill=c("red", "blue")), 
        direction="v", 
        main="Mosaic plot of device branding & register")
 


#https://stats.stackexchange.com/questions/81483/warning-in-r-chi-squared-approximation-may-be-incorrect
#카이제곱검정 http://rfriend.tistory.com/112

library(gmodels) #Tools for Model Fitting #http://rfriend.tistory.com/120
library(vcd) 

attach(dt) #factor, chr로 정제된 데이터 넣기 
CrossTable(mobileDeviceBranding, isRegister, # crosstable = 교차분석 http://dbrang.tistory.com/1067 
            expected = TRUE, # expected frequency
            chisq = TRUE) # chisq-test 
detach(dt)

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
write.csv(data, file="ga360-data-20180704-2.csv", row.names = TRUE) 

# ----------------------------------------------------------------------- # 
# ----------------------------------------------------------------------- # 

# use quantiles for cut https://stackoverflow.com/questions/40380112/categorize-continuous-variable-with-dplyr

#### categorize continuous varialbe with dplyr ####
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



-- 

chi <-dt[, c(10:15)] #열 


#sampling 
# http://blog.acronym.co.kr/587

set.seed(!)
library(sample)
dnorm
