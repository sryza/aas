# Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
#
# See LICENSE file for further information.

install.packages("rgl") # First time only
library(rgl)

# Read clusters and data separately
clusters_data <- read.csv(pipe("hadoop fs -cat /user/ds/sample/*"))
clusters <- clusters_data[1]
data <- data.matrix(clusters_data[-c(1)])
rm(clusters_data)

# Make a random 3D projection and normalize
random_projection <- matrix(data = rnorm(3*ncol(data)), ncol = 3)
random_projection_norm <- random_projection / sqrt(rowSums(random_projection*random_projection))

# Project and make a new data frame
projected_data <- data.frame(data %*% random_projection_norm)

num_clusters <- nrow(unique(clusters))
palette <- rainbow(num_clusters)
colors = sapply(clusters, function(c) palette[c])
plot3d(projected_data, col = colors, size = 10)
