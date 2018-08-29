if(!eval(parse(text="require(pacman)")))
{
  install.packages("pacman")
  eval(parse(text="require(pacman)"))
}

pacman::p_load(
  ggplot2, # Charts
  ggthemes, # Nice charts
)

#our own tools for visualization, h2o handling, and datetime functionalities
devtools::install_github("viadee/viadeeDataViz")
require(viadeedataviz)

# dataset
data <- read.csv("Skalierungstests.csv", sep=";")

#data$Slaves = as.factor(data$Slaves)
data$X...Cores.used = as.factor(data$X...Cores.used)

#plot(data)

viadeePlot(ggplot(data=data,aes(x=data$X...Cores.used,y=data$Duration.in.min)))+
  geom_bar(stat="identity",color="white",fill="white")+
  geom_text(aes(label=data$Duration.in.min),position="stack",vjust=2)+
  scale_y_continuous(limits=c(0, 7))+
  xlab("Anzahl Kerne")+
  ylab("Dauer in Minuten")+
  theme(axis.text.x=element_text(size=rel(1.3)))+
  theme(axis.text.y=element_text(size=rel(1.3)))+
  theme(axis.title.x=element_text(size=rel(1.3)))+
  theme(axis.title.y=element_text(size=rel(1.3)))
