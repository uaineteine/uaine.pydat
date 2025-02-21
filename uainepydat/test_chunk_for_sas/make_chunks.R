# Load the haven package
library(haven)

# Create a dataset with 10,000 rows and a single column with the value "A"
data <- data.frame(var = rep("A", 100000000))

# Save the dataset as a .sas7bdat file
write_sas(data, "dataset.sas7bdat")
