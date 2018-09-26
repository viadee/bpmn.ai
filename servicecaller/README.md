# Service Call Demo

Die Service Call Demo ist eine Spring Boot Anwendung, die einen Camunda Workflow initialisiert und eine Vorhersage für ein trainiertes Modell abholt. Die Vorhersage wird im Prozess gespeichert und weiterverarbeitet.

## Beispielprozess

Der Prozess besteht aus 2 Aktivitäten die mit einer Java-Klasse verknüpft sind.
Im Startevent werden Formulardaten abgefragt, die ggf. als Input für die Prognose dienen. In diesem Fall sind das folgende:

* Kundennummer
* Kundenname
* Kundenalter*
* Jahreseinkommen*
* Kreditsumme*

\* input-Variable für das Prognosemodell.

![alt text](process.png "Logo Title Text 1")

Nach Eingabe und Absenden der Formulardaten, wird die Aktivität "Vorhersage holen" aufgerufen. Die Aktivität ist mit der Java-Klasse "PredictionCall" verknüpft. In dieser Klasse wird die DelegateExecution-Instanz, welche alle Prozessvariablen und einige Metadaten enthält, zum Anfragen der Vorhersage verwendet. Das Ergebnis der Vorhersage wird als neue Prozessinstanzvariable gespeichert.

## Implementierung

### Anfrage senden

```java
public class PredictionCall implements JavaDelegate {
    public void execute(DelegateExecution execution) throws Exception {

        JSONUtils ju = new JSONUtils();
        String processData = ju.genJSONStr(execution); // Format vorbereiten

    ...

```

Zu Beginn wird die Methode *genJSONStr* aufgerufen, die die Prozessinstanzvariablen in ein konsistentes JSON-Schema formatiert und zusammenfässt. Im KI-Büro wurde sich auf folgende Struktur geeinigt:

```JSON
{
  "meta": {
    "procDefID": "prediction-demo:1:ccf1a660-c185-11e8-a4c7-f01faf15d38c",
    "procInstID": "fb2531f1-c185-11e8-a4c7-f01faf15d38c"
  },
  "content": {
    "Kundennummer": 1234,
    "Kreditsumme": 300000,
    "Jahreseinkommen": 45000,
    "Name": "Alex",
    "Kundenalter": 22
  }
}

```
Es gibt also zwei JSON-Objekte *meta* und *content*, wobei *content* die Prozessvariablen enthält und *meta* die Metadaten.

Als nächstes wird ein Post ausgeführt, wobei der Header das JSON-Format zugewiesen bekommt. Die formatierten Daten werden als ByteArrayEntity in den Body eingefügt und abgeschickt.

```JAVA
        post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        HttpEntity body = new ByteArrayEntity(payload.getBytes());
        post.setEntity(body);

```

### Vorhersage erhalten

Die Antwort vom Server liegt ebenfalls im JSON-Format vor und muss daher geparsed werden. Anschließend wird das Resultat als neue Prozessvariable gespeichert.

```JAVA
        JsonParser parser = new JsonParser();
        JsonObject predictionObject = parser.parse(prediction).getAsJsonObject();
        boolean predictionResult = predictionObject.get("predictionResult").getAsBoolean();
        execution.setVariable("predictionResult", predictionResult);
```


## Beispiel für das Vorhersagemodell

Das Vorhersagemodell hat als Predictor-Variablen das Kundenalter, Jahreseinkommen und die Kreditsumme. Als Responsevariable steht die Kreditwürdigkeit (true/false). Der Datenbestand wurde mit mockaroo erzeugt und dient nur Testzwekcen. Anschließend Modell wurde als MOJO exportiert und im ServiceController eingesetzt. 


