from Utilities.seabass import Seabass

Seabass().create_csv(sb_file_directory='',
                     kd=False, # Including all Kd columns requires extremely long export time
                     additional_columns=['kd', 'kd_se', 'kdpar', 'sal'])
