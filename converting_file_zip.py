import zipfile

def convertFiletoZipfile(file_name:str, zip_file_name:str):
    try:
        with zipfile.ZipFile(zip_file_name, 'w') as file:
            file.write(file_name)
            print(f'file {file_name} is successfully converted into zipfile {zip_file_name}')
    except Exception as e:
        print(f'error occured while converting file to zip file: {e}')