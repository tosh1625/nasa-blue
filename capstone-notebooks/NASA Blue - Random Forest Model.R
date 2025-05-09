library(tidyverse)
library(randomForest)
library(mice)
library(ggplot2)
library(caret)

setwd("C:\\Users\\ippis\\Desktop\\Masters\\Capstone\\Main Scripts\\")

source('functions.R')

hydro_df <- read.csv("data\\globe_water_transparency_20250505.csv")



# Transform the full hydrology dataset
# When multiple measurements are available for the same protocol, use the most common
# Label how the protocol was measured. NAs will be imputed

data <- 
hydro_df %>%
  filter(
    is.na(latitude.sample.) == F,
    is.na(elevation.sample.) == F
  ) %>%
  mutate(
    RowId = row_number(),
    measured_on = as.Date(measured_on, tryFormats = c("%Y-%m-%d")),
    
    Alkalinity = ifelse(is.na(hydrology.alkalinities.alkalinity.via.direct..mgl.), hydrology.alkalinities.alkalinity.via.drop..mgl. , hydrology.alkalinities.alkalinity.via.direct..mgl.),
    Alkalinity_Method = ifelse(is.na(hydrology.alkalinities.alkalinity.via.direct..mgl.), "Via_Drop", "Via_Direct"),
    Alkalinity_Method = ifelse(is.na(Alkalinity), "Imputed", Alkalinity_Method),
    
    Conductivity = conductivities.conductivity.micro.siemens.per..cm.,
    Conductivity_Method = ifelse(is.na(Conductivity), "Imputed", "Micri_Siemens_CM"),
    
    Dissolved_Oxygens = ifelse(is.na(dissolved.oxygens.dissolved.oxygen.via.kit..mgl.), dissolved.oxygens.dissolved.oxygen.via.probe..mgl., dissolved.oxygens.dissolved.oxygen.via.kit..mgl.),
    Dissolved_Oxygens_Method = ifelse(is.na(dissolved.oxygens.dissolved.oxygen.via.kit..mgl.), "Via_Probe", "Via_Kit"),
    Dissolved_Oxygens_Method = ifelse(is.na(Dissolved_Oxygens), "Imputed", Dissolved_Oxygens_Method),
    
    Nitrates = ifelse(is.na(nitrates.nitrate.and.nitrite..mgl.),nitrates.nitrite.only..mgl. ,nitrates.nitrate.and.nitrite..mgl.),
    Nitrates_Method = ifelse(is.na(nitrates.nitrate.and.nitrite..mgl.),"Nitrite_Only","Nitrate_and_Nitrite"),
    Nitrates_Method = ifelse(is.na(Nitrates), "Imputed", Nitrates_Method),
    
    pH = hydrology.phs.ph,
    pH_Method = ifelse(is.na(pH), "Imputed", hydrology.phs.ph.method),
    
    Salinity = salinities.salinity.via.hydrometer..ppt.,
    Salinity_Method = ifelse(is.na(Salinity), "Imputed", "Via_Hydrometer"),
    
    Water_Temperature = water.temperatures.water.temp..deg.C.,
    Water_Temperature_Method = ifelse(is.na(Water_Temperature), "Imputed", "Measured"),
    
    #Transparency = abs(ifelse(is.na(transparencies.tube.image.disappearance..cm.), transparencies.transparency.disk.image.disappearance..m.*100,transparencies.tube.image.disappearance..cm.)),
    #Transparency_Method = ifelse(is.na(transparencies.tube.image.disappearance..cm.), "Disk" , "Tube"),
    
    #Transparency = abs(as.numeric(transparencies.transparency.disk.image.disappearance..m.*100)),
    #Transparency_Method = "Disk",
    
    Transparency = abs(as.numeric(transparencies.tube.image.disappearance..cm.)),
    Transparency_Method = "Tube",
    
    Transparency_Saturated = as.character(ifelse(Transparency_Method == "Tube", transparencies.tube.image.does.not.disappear, transparencies.transparency.disk.does.not.disappear)),
    Transparency_Saturated = ifelse(Transparency_Saturated== 'True', 'saturated', 'not-saturated'),
    
    
    Season = get_season(measured_on, latitude.sample.),
    
    water_body_type = ifelse(water_body_type == "", "unknown", water_body_type),
    bank_material = ifelse(bank_material == "", "Unknown", bank_material),
    bedrock_type = ifelse(bedrock_type == "", "Unknown", bedrock_type),
    water_body_source.hes_yellow. = ifelse(water_body_source.hes_yellow. %in% c("", "Insufficient"), "Inconclusive", water_body_source.hes_yellow.),
    
    latitude.sample. = abs(latitude.sample.),
    
    Transparency = 1/Transparency
  ) %>%
  select(
    RowId,
    site_id,
    measured_on,
    latitude.sample.,
    elevation.sample.,
    water_body_type,
    #bank_to_bank_distance,
    can_see_bottom,
    bank_material,
    bedrock_type,
    freshwater_habitat,
    salt_habitat,
    
    #"depth" = depth.hes_yellow.,
    "distance_from_water" = distance_from_water.hes_yellow.,
    "water_body_source" = water_body_source.hes_yellow.,
    
    Alkalinity,
    Alkalinity_Method,
    Conductivity,
    Conductivity_Method,
    Dissolved_Oxygens,
    Dissolved_Oxygens_Method,
    Nitrates,
    Nitrates_Method,
    pH,
    pH_Method,
    Salinity,
    Salinity_Method,
    Water_Temperature,
    Water_Temperature_Method,
    Transparency,
    #Transparency_Method,
    Transparency_Saturated,
    Season
  ) %>%
  filter(
    water_body_source != "Inconclusive",
    is.na(Transparency)==F
  )



candidate_sites <- 
  data %>%
  filter(
    distance_from_water < 1000,
    Transparency_Saturated == 'not-saturated'
  ) %>%
  group_by(
    site_id
  ) %>%
  summarize(
    n = n(),
    iqr = IQR(Transparency)
  ) %>% ungroup() %>%
  filter(
    iqr > 0,
    n >= 10,
    #site_id != "11516"
  ) %>% 
  arrange(
    desc(n)
  ) %>%
  select(site_id)



# Separate out the habitats
# Split the two 'has_info' columns by commas and create new binary columns
unique_values <-
  data %>%
  # Split each 'has_info' column into lists of individual phrases
  mutate(
    freshwater_habitat = strsplit(as.character(freshwater_habitat), ", "),
    salt_habitat = strsplit(as.character(salt_habitat), ", ")
  ) %>%
  # Combine the two lists into one column
  mutate(all_has_info = map2(freshwater_habitat, salt_habitat, ~ c(.x, .y))) %>%
  unnest(cols = c(all_has_info)) %>%  # Flatten the list into rows
  distinct()

unique_values <- unique(unique_values$all_has_info)

# Create a new binary column for each unique value
data <- data %>%
  mutate(
    freshwater_habitat = strsplit(as.character(freshwater_habitat), ", "),  # Split the first column
    salt_habitat = strsplit(as.character(salt_habitat), ", ")   # Split the second column
  ) 

for (value in unique_values) {
  column_name <- paste("has_", gsub(" ", "_", value), sep = "")
  
  data[[column_name]] <- sapply(1:nrow(data), function(i) {
    fw <- data$freshwater_habitat[[i]]
    sw <- data$salt_habitat[[i]]
    
    # Check if both habitats are empty
    if ((length(fw) == 0 || is.null(fw)) && (length(sw) == 0 || is.null(sw))) {
      return("Unknown")
    } else if (value %in% fw || value %in% sw) {
      return("Yes")
    } else {
      return("No")
    }
  })
}

data <-
  data %>%
  select(
    -salt_habitat,
    -freshwater_habitat
  )


names(data) <- gsub("has_","",names(data))

rm(column_name)
rm(unique_values)
rm(value)
  


water_body_types <- unique(data$water_body_type)
imputed_list <- list()

data$distance_from_water <- NULL

for (type in water_body_types) {
  subset_df <- data %>% filter(water_body_type == type)
  
  # Identify columns to exclude (e.g., site_id, RowId)
  exclude_vars <- c("site_id", "RowId", "measured_on", "Transparency", "Transparency_Saturated")
  
  # Setup 'where' matrix to only impute untrusted rows
  where_matrix <- is.na(subset_df)
  #where_matrix[subset_df$site_id %in% candidate_sites, ] <- FALSE
  
  # Initialize predictor matrix
  pred_matrix <- make.predictorMatrix(subset_df)
  
  # Remove site_id and RowId as predictors and from being imputed
  pred_matrix[exclude_vars, ] <- 0     # Don't impute these
  pred_matrix[, exclude_vars] <- 0     # Don't use them to predict others
  
  # Run imputation
  imp <- mice(
    data = subset_df,
    m = 1,
    maxit = 5,
    method = "pmm",
    where = where_matrix,
    predictorMatrix = pred_matrix,
    seed = 123
  )
  
  imputed_df <- complete(imp)
  imputed_list[[type]] <- imputed_df
  
  print(paste("Imputed:", type))
}

data_imputed <- bind_rows(imputed_list)

rm(pred_matrix)
rm(imp)
rm(imputed_df)
rm(imputed_list)
rm(where_matrix)
rm(exclude_vars)
rm(type)
rm(water_body_types)
rm(subset_df)

data_imputed %>%
  group_by(
    Dissolved_Oxygens_Method
  ) %>%
  summarize(
    Records = n(),
    Min = min(Dissolved_Oxygens),
    Mean = mean(Dissolved_Oxygens),
    Median = median(Dissolved_Oxygens),
    Max = max(Dissolved_Oxygens)
  )

data_model <-
  data_imputed %>%
  filter(
    Transparency_Saturated == "not-saturated",
    Transparency > 0,
    is.infinite(Transparency) == F,
    site_id %in% c(candidate_sites$site_id)
  ) %>%
  select(
    -RowId,
    -site_id,
    -measured_on,
    -Transparency_Saturated,
  ) #%>% select(
    #-Alkalinity,  
    #-Conductivity,  
    #-Dissolved_Oxygens,  
    #-Nitrates,  
    #-pH,  
    #-Salinity,  
    #-Water_Temperature
    
    #-latitude.sample.
  #)

data_model_non_candidates <-
  data_imputed %>%
  filter(
    Transparency_Saturated == "not-saturated",
    Transparency > 0,
    is.infinite(Transparency) == F,
    !site_id %in% c(candidate_sites$site_id)
  ) %>%
  select(
    -RowId,
    -site_id,
    -measured_on,
    -Transparency_Saturated
  )

data_model <- data_model %>% mutate(across(where(is.character), as.factor))
data_model_non_candidates <- data_model_non_candidates %>% mutate(across(where(is.character), as.factor))


train_indices <- sample(1:nrow(data_model), 0.8 * nrow(data_model))

data_train <- data_model[train_indices, ]
data_test <- data_model[-train_indices, ]

data_test_non_candidates <- sample_n(data_model_non_candidates, nrow(data_test))


control <- trainControl(method = "cv", number = 10)

random_forest_model <- randomForest(Transparency ~ .,
                                    data = data_train,
                                    method = "rf",
                                    trControl = control,
                                    tuneLength = 5)

print(random_forest_model)

plot(random_forest_model, main = "Random Forest Model")

varImpPlot(random_forest_model, main = "Variable Importance Plot")


# Make predictions
transparency_predictions <- predict(random_forest_model, newdata = data_test)

plot_data <- data.frame(Actual = data_test$Transparency, Predicted = transparency_predictions)

plot_lm <- lm(Actual ~ Predicted, data = plot_data)
intercept <- coef(plot_lm)[1]
slope <- coef(plot_lm)[2]
rsq <- summary(plot_lm)$r.squared


set.seed(1)
plot_data$Residual <- plot_data$Actual - plot_data$Predicted
plot_data <-  plot_data %>% filter(Predicted <= .15, Actual <= .15) %>% sample_n(250)



eq_label <- paste0("y = ", round(intercept,2), "+ ", round(slope,2),"x")
ggplot(plot_data, aes(x = Predicted, y = Actual, color = Residual)) +
  geom_point() +
  geom_smooth(method = "lm", se = TRUE, color = "red", linetype = "dashed") +
  annotate("text", x = .025, y = .14, label = eq_label, hjust = 0, size = 5, fontface = "italic") +
  annotate("text", x = .025, y = .13, label = paste0("R\u00b2"," = ", round(rsq,2)), hjust = 0, size = 5, fontface = "italic") +
  labs(title = "Actual vs Predicted Values from Random Forest", x = "Predicted Values 1/Measurement(CM)", y = "Actual Values 1/Measurement(CM)") +
  xlim(0, 0.15) +
  ylim(0, 0.15) +
  theme_minimal() +
  scale_color_gradient2(low = "blue", mid = "grey", high = "orange", midpoint = 0)




common_cols <- intersect(names(data_imputed), names(data_model))


# Loop through common columns and update types 
for (col in common_cols) {
  target_class <- class(data_model[[col]])
  
  if (length(target_class) > 1) {
    class(data_imputed[[col]]) <- target_class
  } else {
    data_imputed[[col]] <- match.fun(paste0("as.", target_class))(data_imputed[[col]])
  }
}

data_imputed$Predicted <- predict(random_forest_model, newdata = data_imputed)
data_imputed$residual <- abs(data_imputed$Predicted - data_imputed$Transparency)

predictions <- predict(random_forest_model, newdata = data_train)
residuals <- data_train$Transparency - predictions

plot(predictions, residuals,
     xlab = "Predicted Transparency",
     ylab = "Residuals",
     main = "Residuals vs Predicted",
     pch = 16, col = "darkblue")
abline(h = 0, col = "red", lty = 2)

hist(residuals, 
     breaks = 30,
     main = "Histogram of Residuals",
     xlab = "Residual",
     xlim = c(-.05, .05),
     col = "lightblue", border = "white")

qqnorm(residuals)
qqline(residuals, col = "red")



data_imputed <-
  data_imputed %>%
  mutate(
    residual_saturated = ifelse(Transparency_Saturated == 'saturated' #& Transparency_Method == "Tube" 
                                & Transparency <= Predicted, 0, residual)
  )




imputed_counts <- data_imputed %>%
  mutate(across(everything(), as.character)) %>%  # ensure all columns are character
  rowwise() %>%
  mutate(imputed_count = sum(str_detect(c_across(everything()), "Imputed"))) %>%
  ungroup() %>%
  group_by(site_id) %>%
  summarise(
    total_imputed = sum(imputed_count, na.rm = TRUE),
    total_rows = n(),
    impute_ratio = total_imputed / total_rows
  )

imputed_counts$site_id <- as.integer(imputed_counts$site_id)

sites_residuals <-
  data_imputed %>%
  group_by(
    site_id
  ) %>%
  summarize(
    n=n(),
    non_saturated_n = sum(Transparency_Saturated=='not-saturated'),
    
    avg_residual = mean(residual),
    avg_residual_saturated = mean(residual_saturated),
    
    iqr = IQR(Transparency)
  ) %>%
  arrange(
    desc(non_saturated_n)
  ) %>% ungroup() %>%
  mutate(
    trained_site = ifelse(site_id %in% unique(candidate_sites$site_id), "Y", "N")
  ) %>%
  left_join(
    imputed_counts[,c("site_id", "impute_ratio")],
    by = c("site_id" = "site_id")
  )






i <- 1
site_df <- 
  data_imputed %>%
  filter(site_id == 	 	
           11516
  )


rmse <- round(sqrt(mean((site_df$Predicted - site_df$Transparency)^2)),4)


ggplot(site_df, aes(x = measured_on)) +
  geom_line(aes(y = Transparency, color = "Observed Transparency"), size = 1) +
  geom_line(aes(y = Predicted, color = "Predicted Transparency"), size = 1) +
  theme_minimal() +
  labs(
    title = paste0("Site ", site_df[i, "site_id"], " - RMSE: ", rmse, " - Observations: ", nrow(site_df), " - Included in Training"),
    x = "Date of Measurement",
    y = "1/Transparency (cm)",
    color = "Line Type"  # this will be the legend title
  ) +
  scale_color_manual(values = c("Observed Transparency" = "steelblue", "Predicted Transparency" = "orange"))


  