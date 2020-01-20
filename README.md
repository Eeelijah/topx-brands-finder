# Top X Brands Finder

Инструмент для поиска топ Х брендов пользователей сайта.  

Допущения, на основе которых разрабатывалось данное приложение:
1. Логи и справочник хранятся на файловой системе в csv формате  
2. Запись результатов работы приложения необходимо также сохранять в формате csv  

## Порядок запуска приложения
1. Склонировать репозиторий
2. Выполнить из корневой папки проекта mvn clean install assembly:single
3. Собранный *jar-with-dependencies.jar закинуть на app-ноду
4. Подготовить app.conf и brands.txt (дефолтные версии лежат в ресурсах) и положить рядом с джарником
## Параметры
### app
dictDir (String) - путь к папке со словарем brand_name,item_id  
logsDir (String) - путь к папке с логами  
destDir (String) - путь к папке для записи результатов  
topX (int) - число топ брендов, которые необходимо посчитать  
mode (String) - режим запуска приложения - тестовый (подсчет сгенерированных, тестовых данных) или продуктивный test/prod   
### spark
master (String) - master url  
queue (String) - очередь yarn  
driverMemory (String) - память, выделяемая драйверу  
executorMemory (String) - память, выделяемая воркерам  
executorCores (String) - число ядер, выделяемых воркерам  
executorNum (String) - число воркеров  
maxExecutors (String) - максимальное количество воркеров  
thriftUrl (String)  
### test
csvSampleName (String) - шаблон имени для csv файла  
samplesDir (String) - путь к папке с тестовыми данными  
brandNames (String) - путь к файлу brands.txt с названиями брендов для генерации данных  
logsDir (String) - путь к папке для генерации логов  
dictionaryDir (String) - путь к папке для генерации словаря brand_name : item_id  
logsHeader (String) - шапка csv файла с логами  
dictHeader (String) - шапка csv файла со словарем  
baseUrl (String) - базовый url для генерации ссылок на товары  
daysToGenerate (int) - количество дней/партиций для генерации логов  
numberOfUsers (int) - количество пользователей/посетителей сайта для генерации логов

  