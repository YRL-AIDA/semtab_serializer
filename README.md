
# <div align="center"> SemTab Serializer </div>

**SemTab Serializer**

## Version

0.1

## Directory Structure

\#TODO

## Usage

\#TODO
Функция check_type_comprehensive принимает как одиночные значения, так и целые pd.Series (столбцы).
Она возвращает кортеж, содержащий наибольший тип данных и количество пропусков, найденных в анализируемом наборе. Если большинство за типом int,но есть хотя бы один float, то большинство float.
если функция df_to_duckling не нашла заложенный в ней паттерн, то информацию будет обрабатывать check_type_comprehensive.
<img width="717" height="880" alt="image" src="https://github.com/user-attachments/assets/403ca20b-7194-472c-b649-60adf6179eab" />
## Authors

\#TODO


# Docker Setup for Duckling

## Prerequisites
Run the following commands in your terminal to install system dependencies and add your user to the Docker group:

```bash
# Update package list and install required dependencies
sudo apt-get update
sudo apt-get install -y libgmp-dev libpcre3-dev build-essential

# Add user 'master' to the docker group
sudo usermod -aG docker master

#run docker
docker run -d -p 8000:8000 --name duckling rasa/duckling

# Check container status
docker ps