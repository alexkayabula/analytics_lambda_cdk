# import the datetime module
import datetime 
  
def formatDate(input):
    # input datetime in string format YYYYMMDD

    # format
    format = '%Y%m%d'
    
    # convert from string format to datetime format
    dateFormat = datetime.datetime.strptime(input, format)
    
    # get the date from the datetime using date() 
    date = dateFormat.date()
    return date