from Seabass.seabass import Seabass

Seabass().create_csv(sb_file_directory='/path/to/your/seabass_curated', chl_all=False, depth_all=False,
                     kd_all=False, rrs_all=True, spm_all=False, preview_column_list=False, additional_columns=
                     ['chl', 'chl_a', 'chl_mean', 'chl_sd', 'chl_se', 'depth', 'kd', 'kd_se','kdpar', 'oxygen', 'par',
                      'phaeo', 'sal', 'spm', 'sz', 'water_depth',
                      # Kd 440±2nm
                      'kd438', 'kd438.0', 'kd438.0_bincount', 'kd438.0_se', 'kd438.0_unc', 'kd439', 'kd440', 'kd440.0',
                      'kd440.0_bincount', 'kd440.0_se', 'kd440.0_unc', 'kd441', 'kd441.0', 'kd441.0_bincount',
                      'kd441.0_se', 'kd441.0_unc', 'kd442', 'kd442.0', 'kd442.0_bincount', 'kd442.0_se', 'kd442.0_unc',
                      'kd442.5', 'kd442.8', 'kd442.9',
                      # Kd 490±2nm
                      'kd488', 'kd488.0', 'kd488.0_bincount', 'kd488.0_se', 'kd488.0_unc', 'kd489', 'kd489.0',
                      'kd489.0_bincount', 'kd489.0_se', 'kd489.0_unc', 'kd489.4', 'kd489.6', 'kd489.7', 'kd489.9',
                      'kd490', 'kd490.0', 'kd490.0_bincount', 'kd490.0_se', 'kd490.0_unc', 'kd490.1', 'kd490.4',
                      'kd490.8', 'kd491', 'kd491.0', 'kd491.0_bincount', 'kd491.0_se', 'kd491.0_unc', 'kd492',
                      # Kd 510±2nm
                      'kd508', 'kd508.0', 'kd508.0_bincount', 'kd508.0_se', 'kd508.0_unc', 'kd509', 'kd509.0',
                      'kd509.0_bincount', 'kd509.0_se', 'kd509.0_unc', 'kd509.3', 'kd509.6', 'kd509.7', 'kd510',
                      'kd510.0', 'kd510.0_bincount', 'kd510.0_se', 'kd510.0_unc', 'kd510.7', 'kd511', 'kd511.0',
                      'kd511.0_bincount', 'kd511.0_se', 'kd511.0_unc', 'kd512',
                      # Kd 550±2nm
                      'kd548', 'kd548.0', 'kd548.0_bincount', 'kd548.0_se', 'kd548.0_unc', 'kd549', 'kd549.0',
                      'kd549.0_bincount', 'kd549.0_se', 'kd549.0_unc', 'kd550', 'kd550.0', 'kd550.0_bincount',
                      'kd550.0_se', 'kd550.0_unc', 'kd551', 'kd551.0', 'kd551.0_bincount', 'kd551.0_se', 'kd551.0_unc',
                      'kd552', 'kd552.0', 'kd552.0_bincount', 'kd552.0_se', 'kd552.0_unc'])

