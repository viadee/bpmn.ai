# BPMN.AI

Verwendung von Standard-Prozessdaten (bspw. Camunda) im Rahmen  einer standardisierten Pipeline: Extraktion, Transformation und Aufarbeitung. Anschließend werden die Daten verwendet um ein Machine Learning durchzuführne.

Aktuell umfasst BPMN.AI zwei Projekte. Der Spark-Importer ist unsere Lösung für die beschriebene Pipeline. Der Service Caller ist eine Demo, wie ein Abrufen und Verwenden von Prognoseergebnissen in einem Camunda-Workflow aussehen könnte.

## spark-importer

Der Spark-Importer enthält drei Apache Spark-Anwendungen, die der Aufgabe dienen, Daten aus der Camunda-Engine zu übernehmen und sie in eine Data-Mining-Tabelle zu überführen, die eine Zeile pro Prozessinstanz mit zusätzlichen Spalten für jede Prozessvariable enthält. Diese Data-Mining-Tabelle wird dann verwendet, um einen Machine Learning Algorithmus zu trainieren, um bestimmte Ereignisse des Prozesses in der Zukunft vorherzusagen.
Für die folgenden Anwendungen stehen zur Verfügung:

* SparkImporterCSVApplication
* SparkImporterKafkaImportApplication
* SparkImporterKafkaDataProcessingApplication

Jede dieser Anwendungen erfüllt einen anderen Zweck.

### Datenpipeline

Die folgende Grafik zeigt die Pipeline, durch die die Daten von Camunda zur Machine Learning Engine fließen. Jede der drei Anwendungen dient einem bestimmten Zweck und Anwendungsfällen rund um den Import in, die Datenaggregation und -transformation innerhalb und den Export von Daten aus Apache Spark.

![alt text](./spark-importer/doc/SparkImporterApplicationFlow.png "SparkImporterCSVApplication Pipeline")

### SparkImporterCSVApplication

Diese Anwendung (Anwendungsklasse: CSVImportAndProcessingApplication) nimmt Daten aus einem CSV-Export von Camunda-History-Datenbanktabellen auf und aggregiert sie zu einer Data-Mining-Tabelle. Das Ergebnis ist auch eine CSV-Datei mit der Data-Mining-Tabellenstruktur.

### SparkImporterKafkaImportApplication

Diese Anwendung (Anwendungsklasse: KafkaImportApplication) ruft Daten von Kafka ab, in denen drei Warteschlangen zur Verfügung gestellt wurden und mit Daten aus dem History-Ereignishandler von Camunda gefüllt werden:

* processInstance: gefüllt mit Ereignissen auf der Ebene der Prozessinstanz
* activityInstance: gefüllt mit Ereignissen auf der Ebene der Activity-Instanz
* variableUpdate: gefüllt mit Ereignissen, die passieren, wenn eine Variable in irgendeiner Weise aktualisiert wird.

Die abgerufenen Daten werden dann an einem definierten Ort als Parkettdateien gespeichert. Es findet keine Datenverarbeitung durch diese Anwendung statt, da sie als Spark-Anwendung laufen kann, die ständig Daten aus Kafka-Streams empfängt.

### SparkImporterKafkaDataProcessingApplication 

Diese Anwendung (Anwendungsklasse: SparkImporterKafkaDataProcessingApplication) ruft Daten aus einem Kafka-Import ab. Die Daten durchlaufen die gleichen Schritte wie in der CSV-Import- und Verarbeitungsanwendung, es ist nur eine separate Anwendung, da sie eine andere Eingabe als der CSV-Fall haben.

## servicecaller

Die Service Call Demo ist eine Spring Boot Anwendung, die einen Camunda Workflow initialisiert und eine Vorhersage für ein trainiertes Modell abholt. Die Vorhersage wird im Prozess gespeichert und weiterverarbeitet. 


