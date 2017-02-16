# Copyright 2015 and onwards Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
#
# See LICENSE file for further information.

# This block only needed if running via RStudio:
# If not set already, set to a Spark distro home directory, and Java home dir
Sys.setenv(SPARK_HOME = "/path/to/spark")
# Sys.setenv(JAVA_HOME = "/path/to/java")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
# Set this appropriately for your cluster
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "4g"))

clusters_data <- read.df("/path/to/kddcup.data", "csv",
                         inferSchema = "true", header = "false")
colnames(clusters_data) <- c(
  "duration", "protocol_type", "service", "flag",
  "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
  "hot", "num_failed_logins", "logged_in", "num_compromised",
  "root_shell", "su_attempted", "num_root", "num_file_creations",
  "num_shells", "num_access_files", "num_outbound_cmds",
  "is_host_login", "is_guest_login", "count", "srv_count",
  "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
  "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
  "dst_host_count", "dst_host_srv_count",
  "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
  "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
  "dst_host_serror_rate", "dst_host_srv_serror_rate",
  "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
  "label")

numeric_only <- cache(drop(clusters_data,
                           c("protocol_type", "service", "flag", "label")))

kmeans_model <- spark.kmeans(numeric_only, ~ .,
                             k = 100, maxIter = 40, initMode = "k-means||")

clustering <- predict(kmeans_model, numeric_only)
clustering_sample <- collect(sample(clustering, FALSE, 0.01))

str(clustering_sample)

clusters <- clustering_sample["prediction"]
data <- data.matrix(within(clustering_sample, rm("prediction")))

table(clusters)
# clusters
# 0    11    14    23    25    28    31    33    36    48    64    83    89
# 47146     1     1     4   278   109    42  1190    13     1     2     1     2


install.packages("rgl") # First time only
library(rgl)

# Make a random 3D projection and normalize
random_projection <- matrix(data = rnorm(3*ncol(data)), ncol = 3)
random_projection_norm <-
  random_projection / sqrt(rowSums(random_projection*random_projection))

# Project and make a new data frame
projected_data <- data.frame(data %*% random_projection_norm)

num_clusters <- max(clusters)
palette <- rainbow(num_clusters)
colors = sapply(clusters, function(c) palette[c])
plot3d(projected_data, col = colors, size = 10)

unpersist(numeric_only)
