library(h2o)
h2o.init()

credit.hex <- h2o.importFile(path = "C:/Users/b60/Desktop/KI352/CamundaTestModell/MOCK_DATA.csv", destination_frame = "credit.hex")

summary(credit.hex)

credit.split <- h2o.splitFrame(data = credit.hex, ratios = 0.8)
credit.train <- credit.split[[1]]
credit.test <- credit.split[[2]]

Y <- "Kreditwuerdig"
X <- c("Kundenalter", "Jahreseinkommen", "Kreditsumme")

credit.glm <- h2o.glm(training_frame=credit.train,
                        x=X, y=Y, family = "binomial", alpha = 0.5)

summary(credit.glm)