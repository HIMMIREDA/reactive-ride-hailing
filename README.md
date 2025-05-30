# Programmation Réactive en Java & SpringBoot

## Sommaire
1.  [Introduction](#1-introduction)
2.  [Threads & Processus](#2-threads--processus)
    * [Processus](#21-processus)
    * [Threads](#22-threads)
        * [Modèles du threading](#221-modèles-du-threading)
3.  [Modèle traditionnel vs Modèle réactif](#3-modèle-traditionnel-vs-modèle-réactif)
    * [Architecture et limites du modèle traditionnel (servlet)](#31-architecture-et-limites-du-modèle-traditionnel-servlet)
    * [Principes du modèle réactif](#32-principes-du-modèle-réactif)
4.  [Fondamentaux de la programmation réactive](#4-fondamentaux-de-la-programmation-réactive)
    * [Reactive Manifesto](#41-reactive-manifesto)
    * [L'initiative Reactive Streams vs. JDK9](#42-linitiative-reactive-streams-vs-jdk9)
    * [Concepts clés (Back pressure, Event-loop, Non-blocking I/O)](#43-concepts-clés-back-pressure-event-loop-non-blocking-io)
        * [Back pressure](#431-back-pressure)
        * [Event-loop](#432-event-loop)
        * [Non-blocking IO](#433-non-blocking-io)
5.  [Implémentations Java de la programmation réactive](#5-implémentations-java-de-la-programmation-réactive)
    * [Project Reactor en détail](#51-project-reactor-en-détail)
        * [Reactor Core (Flux vs Mono vs Publisher)](#511-reactor-core-flux-vs-mono-vs-publisher)
        * [Opérateurs essentiels](#512-opérateurs-essentiels)
        * [Schedulers.boundedElastic, Schedulers.parallel, subscribeOn & publishOn](#513-schedulersboundedelastic-schedulersparallel-subscribeon--publishon)
6.  [Spring WebFlux](#6-spring-webflux)
    * [Architecture et principes](#61-architecture-et-principes)
        * [Event-Loop Thread](#611-eventloop-thread) 
        * [Cycle de vie d'une requête dans Spring WebFlux](#612-cycle-de-vie-dune-requete-dans-spring-webflux)
7.  [Persistance réactive avec R2DBC, Reactor Kafka, WebClient](#7-persistance-réactive-avec-r2dbc--reactor-kafka-webclient-)
    * [Principes de R2DBC](#71-principes-de-r2dbc)
    * [Intégration avec Spring Data R2DBC](#72-intégration-avec-spring-data-r2dbc)
    * [Reactor Kafka](#73-reactor-kafka)
    * [Intégration avec Spring Kafka](#74-intégration-avec-spring-kafka)
8.  [POC pratique](#8-poc-pratique)
    * [Pré-requis](#81-pré-requis)
    * [Lancement du POC](#82-lancement-du-poc)
    * [Technologies utilisées](#83-technologies-utilisées)
    * [Fonctionnalités](#84-fonctionnalités)
9.  [Références](#9-références)

## 1. Introduction

Depuis plusieurs années, le développement web en Java repose sur les servlets, un modèle synchrone et bloquant (modèle thread/request). Cependant, ce modèle devient rapidement inefficace dans les applications modernes et les
architectures microservices. Ces dernières nécessitent le traitement d'un grand nombre de requêtes simultanées en temps réel.  
Pour pallier ces contraintes, le modèle réactif a vu le jour, qui se base sur l'asynchronicité et le non-blocage, il permet une utilisation plus efficiente des ressources système.  
Ce POC vise à comparer le modèle traditionnel et l'approche réactive. Il propose une démo basée
sur [Project Reactor](https://projectreactor.io) et [Spring WebFlux](https://docs.spring.io/spring-framework/reference/web/webflux.html), en exposant les bases de programmation réactive et la manière d'intégrer ces outils avec des bases de données réactives.

## 2. Threads & Processus

Avant d'entamer la programmation réactive, nous allons voir comment un système d'exploitation gère les threads d'exécution.

### 2.1 Processus

Un processus correspond à l'exécution d'un programme exécutable. Il se compose de :
<figure>
  <img width="600" src="images/processus_threads.png" alt="Processus" title="Processus">
  <figcaption style="text-align: center;"><strong>Processus</strong> - source: <a href="https://www.scaler.com/topics/linux-thread/">linux-thread</a></figcaption>
</figure>

- **Code (Text)**: contient les instructions du programme à exécuter (en mode read-only).
- **Data**: contient les variables globales et statiques initialisées.
- **Heap**: zone mémoire partagée entre les threads pour stocker les objets dynamiques.
- **Files**: descripteurs de fichiers ouverts par le processus (fichiers, sockets réseau, …).
- **Threads**: unités d'exécution concurrentes au sein du processus, partageant le même espace d'adressage.

### 2.2 Threads

Chaque Processus est constitué d'au moins un thread d'exécution, un thread représente une sous-tâche

Chaque thread possède :
- **Program Counter (PC)**: pointeur vers la prochaine instruction à exécuter.
- **Registers**: registres du processeur contenant les données en cours de traitement.
- **Stack**: zone mémoire privée contenant les variables locales et les paramètres des fonctions.

il existe deux types de threads:

<figure>
  <img width="600" src="images/user_vs_kernel_thread.png" alt="User vs Kernel Level Threads" title="User vs Kernel Level Threads">
  <figcaption style="text-align: center;"><strong>User-Level thread vs Kernel-Level thread</strong> - source: <a href="https://www.scaler.com/topics/user-level-threads-and-kernel-level-threads/">ult-and-klt</a></figcaption>
</figure>

- **User-level threads (ULT)**: ce sont des threads créés et gérés par des programmes ou des bibliothèques, par exemple: les threads Java, les threads créés par le module threading de Python, POSIX threads (pthreads) en C/C++.
- **Kernel-level threads (KLT)**:  ce sont des threads créés et gérés au niveau du noyau du système d'exploitation. Ce sont les tâches qui sont planifiées pour l'exécution sur le CPU.

#### 2.2.1 Modèles du threading

les **User-level threads (ULT)** ne sont pas exécutés directement au niveau du processeur, mais ils sont liés à des **kernel-level threads (KLT)** exécutables.
Il existe plusieurs modèles de threading pour la correspondance entre ULT et KLT:

<figure>
  <img width="600" src="images/ult_klt.png" alt="Modèles du threading" title="Modèles du threading">
  <figcaption style="text-align: center;"><strong>Types des Modèles du threading</strong> - source: <a href="https://www.researchgate.net/figure/Three-types-of-thread-models-Popular-operating-systems-5-22-24-adopt-the_fig1_346379550">Types of thread models</a></figcaption>
</figure>

- **Modèle 1:1**: Chaque **thread utilisateur ULT** est associé à **un thread noyau dédié KLT**.
- **Modèle M:N**: **M threads utilisateur ULT** sont associés à **N threads noyau KLT**.
- **Modèle M:1**: **Tous les threads utilisateur d'un processus ULT** partagent **un seul thread noyau KLT**.

**Java** utilise le modèle 1:1, où chaque thread Java (ULT) est lié à un KLT. En général les KLT sont coûteux à créer et à planifier au niveau du CPU. Par exemple, si un thread Java effectue un appel à une base de données (opération I/O (entrée/sortie)), son KLT associé reste bloqué en attendant la réponse. Pendant cette période, le thread Java reste inactif, ce qui n'est pas optimal en termes d'utilisation des ressources.
C'est ce problème que **la programmation réactive** cherche à résoudre, en proposant un modèle non-bloquant pour gérer efficacement les opérations I/O.

## 3. Modèle traditionnel vs Modèle réactif

### 3.1 Architecture et limites du modèle traditionnel (servlet)

Une application qui applique **le modèle traditionnel des servlets**, telle qu'une application web en Spring MVC déployée sur un conteneur de servlet comme Tomcat, fonctionne de la manière suivante:

Pour chaque requête utilisateur, le serveur :

- Crée un nouveau thread (ou plutôt récupère un thread à partir du threadpool de Tomcat, configuré avec `server.tomcat.threads.max=250` pour embedded Tomcat).
- Assigne ce thread au traitement complet de la requête.
- Attend la fin du traitement avant de rendre le thread disponible.

Le problème survient lorsque le traitement de la requête nécessite des opérations d'entrée/sortie (I/O) comme : la lecture/écriture d'un fichier, l'appel d'un service externe (API, BDD, microservice en GRPC, …). Dans ces situations, le thread reste bloqué sans rien faire en attendant la réponse de l'opération I/O. Cela entraîne une diminution du nombre de threads disponibles dans le threadpool pour traiter les autres requêtes utilisateur, limitant ainsi la capacité de l'application à gérer un nombre élevé de connexions simultanées.

En général, ce modèle ne convient que pour les applications intensives en calcul (**CPU-intensive**) qui utilisent le thread pour effectuer des calculs tout au long du traitement de la requête, telles qu'une web app qui fait des calculs mathématiques complexes ou un système de calcul financier.

<figure>
  <img width="600" src="./images/spring_mvc.png" alt="Spring MVC" title="Spring MVC">
  <figcaption style="text-align: center;"><strong>Spring MVC (modèle servlet)</strong> - source: <a href="https://singhkaushal.medium.com/spring-webflux-eventloop-vs-thread-per-request-model-a42d07ee8502">Webflux vs Thread per request model</a></figcaption>
</figure>

### 3.2 Principes du modèle réactif
Le modèle réactif se base sur une boucle des événements (**Event Loop**), Contrairement au modèle traditionnel, au lieu de créer un thread par requête qui reste bloqué en attendant les opérations I/O, le modèle réactif crée un threadpool avec un nombre fixe de threads appelés **"Event Loop Threads"**. Ces threads traitent uniquement les requêtes à partir d'une file d'événements (**event queue**). Dès qu'une opération bloquante est rencontrée, le thread n'attend pas son résultat. A la place, il enregistre un callback qui s'exécutera ultérieurement quand la réponse sera prête. Entre-temps, le thread est libéré pour traiter d'autres requêtes.

C'est le modèle utilisé par exemple dans NodeJS, Netty (le serveur embarqué par défaut dans 
Spring Webflux).

<figure>
  <img width="600" src="./images/event_loop.png" alt="Event Loop - Async I/O" title="Event Loop - Async I/O">
  <figcaption style="text-align: center;"><strong>Modèle Event Loop</strong> - source: <a href="https://www.baeldung.com/spring-webflux-concurrency">Concurrency in Spring WebFlux</a></figcaption>
</figure>

**Remarque**: La plupart des frameworks et langages qui se reposent sur ce modèle utilisent, afin de ne pas bloquer les event loop threads, des threads supplémentaires appelés worker threads. Ces derniers sont utilisés pour effectuer des calculs intensifs ou exécuter du code issu de bibliothèques proposant des API bloquantes (comme un ancien pilote de base de données). 

## 4. Fondamentaux de la programmation réactive

### 4.1 Reactive Manifesto
(Le manifeste Réactif)[ https://www.reactivemanifesto.org/fr ]  définit un système réactif comme étant un système distribué possédant quatre caractéristiques :
<figure>
  <img width="600" src="./images/reactive_system.png" alt="Système Réactif" title="Système Réactif">
  <figcaption style="text-align: center;"><strong>Caractéristiques d'un système réactif</strong> - source: <a href="https://www.reactivemanifesto.org/fr">Reactive Manifesto</a></figcaption>
</figure>

1. **Responsive**: il répond rapidement en toutes circonstances, si cela est possible.
2. **Elastic**: il reste disponible quelle que soit la charge de travail.
3. **Resilient**: il reste disponible en cas d'erreur.
4. **Message driven**: utilise le passage de messages
   asynchrones entre ses composants afin de garantir leur couplage
   faible, leur isolation, la transparence de localisation ainsi que la
   délégation d’erreurs à d’autres composants

### 4.2 L'initiative Reactive Streams vs. JDK9

[Reactive Streams](https://www.reactive-streams.org/) est une spécification pour le traitement asynchrone des flux de données, principalement destinée à l'écosystème Java. Elle a été adoptée par plusieurs bibliothèques de programmation réactive telles que RxJava 2 et Reactor. Avec l’arrivée de Java 9, les interfaces de cette spécification (`Publisher`, `Subscriber`, `Subscription`, `Processor`) ont été intégrées directement dans le JDK via le package `java.util.concurrent.Flow`, tout en conservant la même sémantique.

**Toutefois, la majorité des bibliothèques ont décidé de rester sur `Reactive Streams` pour garantir une compatibilité avec Java 8.**

**Interfaces de base de Reactive Streams :**

* **`Publisher<T>` :** représente une source de flux de données asynchrones. Son rôle est de produire des éléments et de les envoyer à un ou plusieurs `Subscriber`. Elle ne possède qu'une seule méthode :
   ```java
   public interface Publisher<T> {     
       public void subscribe(Subscriber<? super T> s);
   }
   ```
Cette méthode est appelée par un `Subscriber` pour commencer à recevoir les éléments.

* **`Subscriber<T>` :** représente un consommateur de données émises par un `Publisher`. Il définit les méthodes de callback pour réagir aux événements du flux :
   ```java
   public interface Subscriber<T> {     
       public void onSubscribe(Subscription s);
       public void onNext(T item);
       public void onError(Throwable t);
       public void onComplete();
   }
   ```
   * `onSubscribe(Subscription s)` : appelée lors de la souscription, fournissant un objet de type `Subscription` pour contrôler le flux.
   * `onNext(T item)` : appelée pour chaque élément émis par le `Publisher`.
   * `onError(Throwable t)` : appelée si le `Publisher` termine avec une erreur.
   * `onComplete()` : appelée si le `Publisher` termine avec succès.

* **`Subscription` :** représente la relation entre un `Subscriber` et un `Publisher`. C'est un contrat qui permet au `Subscriber` de contrôler le flux de données qu'il reçoit. Elle définit deux méthodes principales pour la gestion du back pressure :
   ```java
   public interface Subscription {
     public void request(long n);
     public void cancel();
   }
   ```
   * `request(long n)` : permet au `Subscriber` de demander au `Publisher` d'émettre au plus `n` éléments supplémentaires. C'est le mécanisme clé pour implémenter le back pressure.
   * `cancel()` : Permet au `Subscriber` d'annuler sa souscription, signifiant qu'il ne souhaite plus recevoir d'éléments.

* **`Processor<T, R>` :** représente un composant qui est à la fois un `Subscriber<T>` et un `Publisher<R>`. Il peut recevoir des éléments, les traiter, et ensuite publier le résultat. C'est utile pour les étapes intermédiaires dans une chaîne de traitement. L'interface `Processor` étend les deux interfaces :
    ```java
    public interface Processor<T, R> extends Subscriber<T>, Publisher<R> {
    }
    ```
  
### 4.3 Concepts clés (Back pressure, Event-loop, Non-blocking I/O)
#### 4.3.1 Back pressure
<figure>
  <img width="600" src="images/backpressure_1.png" alt="Illustration de la contre-pression 1" title="Contre-pression">
  <img width="600" src="images/backpressure_2.png" alt="Illustration de la contre-pression 2" title="Contre-pression">
  <figcaption style="text-align: center;"><strong>Origines de Back pressure</strong> - source: <a href="https://impurepics.com/posts/2018-05-26-backpressure-origins.html" target="_blank" rel="noopener noreferrer">Back Pressure</a></figcaption>
</figure>

Le **back pressure** est un mécanisme crucial dans la programmation réactive qui permet au consommateur de données de signaler au producteur qu'il est submergé et qu'il ne peut pas traiter les données au rythme où elles sont émises. Sans back pressure, un consommateur lent pourrait être dépassé par un producteur rapide, entraînant des problèmes comme une utilisation excessive de la mémoire ou la perte de données.

#### 4.3.2 Event-loop
L'**Event-loop** est un modèle de programmation asynchrone non bloquant où un seul thread exécute une boucle pour traiter les événements. Au lieu d'attendre la fin des opérations d'E/S, le thread principal lance l'opération et continue. Lorsqu'un résultat est disponible, un événement est déclenché, et le thread principal exécute le gestionnaire (callback) associé. Ceci permet une haute réactivité en évitant le blocage du thread principal.

#### 4.3.3 Non-blocking IO
Les **opérations d'E/S non bloquantes** (**Non-Blocking IO**) permettent à un thread d'initier une opération d'entrée ou de sortie sans attendre sa complétion. Le thread peut ensuite poursuivre d'autres tâches. Une notification est reçue lorsque l'opération est terminée. Contrairement aux E/S bloquantes, cela permet à un nombre limité de threads de gérer de nombreuses opérations concurrentes, améliorant ainsi la performance et la réactivité des applications.


## 5. Implémentations Java de la programmation réactive

Plusieurs librairies Java facilitent l'adoption du paradigme de la programmation réactive. Parmi les plus notables, on trouve :

* **[Project Reactor](https://projectreactor.io/) :** Développé par Pivotal (maintenant VMware), Reactor est une implémentation de la spécification **Reactive Streams**. Il fournit les types fondamentaux **Flux** (pour les séquences de 0 à N éléments) et **Mono** (pour les séquences de 0 ou 1 élément), et met l'accent sur le back pressure et un riche ensemble d'opérateurs pour la manipulation asynchrone de flux de données non bloquants. Reactor est notamment la base réactive de Spring WebFlux. C'est notre sujet de POC avec **Spring WebFlux**.

* **[RxJava](https://github.com/ReactiveX/RxJava) :** Librairie mature suivant le modèle **[ReactiveX](https://reactivex.io/)**, RxJava introduit les **Observable** pour représenter des flux de données asynchrones.

* **[Mutiny](https://smallrye.io/smallrye-mutiny/2.4.0/) :** Développé par Red Hat, Mutiny offre une API réactive plus impérative pour Java et Kotlin, avec les types **Multi** et **Uni** pour les flux et les résultats uniques.

### 5.1 Project Reactor en détail
#### 5.1.1 Reactor Core (Flux vs Mono vs Publisher)
Au coeur de **Project Reactor** et de la spécification **Reactive Streams** se trouve l'interface `Publisher<T>` [Voir la section Reactive Streams vs jdk9](#42-linitiative-reactive-streams-vs-jdk9).
Comme mentionné précédemment, `Publisher<T>` est l'interface fondamentale de Reactive Streams, représentant une source de données asynchrones. Project Reactor fournit deux implémentations principales de cette interface: `Flux` et `Mono`.

* **`Flux<T>` :** Est un `Publisher` qui peut émettre une séquence de **0 à n** éléments. Il est utilisé pour représenter un flux de données qui peut contenir plusieurs éléments, qu'ils arrivent de manière continue (**Stream of events**) ou à la fin d'une opération.

   ```java
    Flux<String> items = Flux.just("Reda", "Java", "Golang", "ILI");
    items.subscribe(item -> System.out.println("item: " + item));
  
   // Supposons que getStockData retourne un Flux<StockData>
   Flux<StockData> stockDataFlux = getStockData("GOOGLE");
   ```

* **`Mono<T>` :** Est également un `Publisher`mais spécialisé pour émettre **0 ou 1** élément. Il est utilisé pour représenter le résultat asynchrone d'une opération qui peut produire zéro ou un résultat, comme la récupération d'un seul enregistrement d'une table de base de données.

    ```java
    Mono<String> user = Mono.just("Reda Himmi");
    user.subscribe(u -> System.out.println("User: " + u));

    Mono<Void> op = Mono.empty().doOnTerminate(() -> System.out.println("Opération terminée."));
    op.subscribe();
    ```

En résumé, `Publisher` est l'interface, `Flux` est l'implémentation pour les séquences de **0 à n** éléments, et `Mono` est l'implémentation pour les séquences de **0 ou 1** élément.

Remarque: **Il est aussi important de noter qu'un `Publisher` ne commence à émettre des éléments que lorsqu'un `Subscriber` s'y abonne via la méthode `subscribe()` ou `subscribe(Consumer<? super T> consumer)`**

#### 5.1.2 Opérateurs essentiels

1.  `just()`: Crée un `Flux` ou un `Mono` qui émet les éléments passés en paramètre, puis se complète. C'est un moyen simple de créer un Publisher statique.
<figure>
<img width="600" src="images/just.png" alt="just()" title="Processus">
  <figcaption style="text-align: center;"><strong>just()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#just-T...-">Flux</a></figcaption>
</figure>

```java
    Flux.just("Hello", "ILI");
    Mono.just("Reda ILI");
```

2. `subscribe()`: Déclenche l'émission des éléments d'un Publisher en attachant un `Subscriber`. C'est l'opération qui active le flux. Elle correspond à l'appel de la méthode `subscribe(Subscriber s)` de l'interface Publisher.
<figure>
<img width="600" src="images/subscribe.png" alt="Subscribe()" title="Subscribe()">
  <figcaption style="text-align: center;"><strong>Subscribe()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#subscribe--">Subscribe()</a></figcaption>
</figure>

```java
    Flux.just("a", "b").subscribe(System.out::println);
```

3. `doOnNext(Consumer<? super T> onNext)` : permet d'exécuter un callback pour chaque élément émis par le `Publisher`, sans modifier l'élément lui-même.
<figure>
  <img width="600" src="images/do_on_next.png" alt="doOnNext()" title="doOnNext()">
  <figcaption style="text-align: center;"><strong>doOnNext()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#doOnNext-java.util.function.Consumer-">doOnNext()</a></figcaption>
</figure>

```java
    Flux.just("Reda", "Himmi")
        .doOnNext(item -> System.out.println("Processing: " + item))
        .subscribe(System.out::println);
```
4. `map(Function<? super T, ? extends R> mapper)` : transforme chaque élément émis par le `Publisher` en appliquant une fonction synchrone.
<figure>
  <img width="600" src="images/map.png" alt="map()" title="map()">
  <figcaption style="text-align: center;"><strong>map()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#map-java.util.function.Function-">map()</a></figcaption>
</figure>

```java
    Flux.just("ili", "reda")
        .map(String::toUpperCase)
        .subscribe(System.out::println);
```

5. `filter(Predicate<? super T> predicate)` : émet uniquement les éléments qui satisfont un prédicat donné.
<figure>
  <img width="600" src="images/filter.png" alt="filter()" title="filter()">
  <figcaption style="text-align: center;"><strong>filter()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#filter-java.util.function.Predicate-">filter()</a></figcaption>
</figure>

```java
    Flux.range(1, 5)
        .filter(n -> n % 2 == 0)
        .subscribe(System.out::println);
```

6. `flatMap(Function<? super T, ? extends Publisher<? extends V>> mapper)` : transforme chaque élément en un `Publisher` et fusionne les flux résultants. L'ordre des éléments émis peut ne pas être conservé.
<figure>
  <img width="600" src="images/flatmap.png" alt="flatMap()" title="flatMap()">
  <figcaption style="text-align: center;"><strong>flatMap()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#flatMap-java.util.function.Function-">flatMap()</a></figcaption>
</figure>

```java
Flux.just(1, 2)
    .flatMap(x -> Mono.just("value " + x))
    .subscribe(System.out::println);
```

7. `concatMap(Function<? super T, ? extends Publisher<? extends V>> mapper)` : Similaire à `flatMap`, mais garantit que les éléments des `Publisher`s internes sont émis dans l'ordre du flux source.
<figure>
  <img width="600" src="images/concatmap.png" alt="concatMap()" title="concatMap()">
  <figcaption style="text-align: center;"><strong>concatMap()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#concatMap-java.util.function.Function-">concatMap()</a></figcaption>
</figure>

```java
Flux.just(1, 2)
    .concatMap(x -> Mono.just("value " + x)
    .delayElement(java.time.Duration.ofMillis(100 * x)))
    .subscribe(System.out::println);  // va afficher: "value: 1" puis "value: 2"
```

8. `delayElements(Duration period)` : introduit un délai entre l'émission de chaque élément.
<figure>
  <img width="600" src="images/delayelements.png" alt="delayElements()" title="delayElements()">
  <figcaption style="text-align: center;"><strong>delayElements()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#delayElements-java.time.Duration-">delayElements()</a></figcaption>
</figure>

```java
    Flux.just("one", "two")
        .delayElements(java.time.Duration.ofSeconds(1))
        .subscribe(System.out::println);
```

9. `take(long n)` : émet seulement les n premiers éléments, puis se complète.
<figure>
  <img width="600" src="images/take.png" alt="take()" title="take()">
  <figcaption style="text-align: center;"><strong>take()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#take-long-">take()</a></figcaption>
</figure>

```java
    Flux.range(1, 10)
        .take(3)
        .subscribe(System.out::println);
```

10. `interval(Duration period)` : crée un Flux qui émet des Longs à intervalles réguliers.
<figure>
  <img width="600" src="images/interval.png" alt="interval()" title="interval()">
  <figcaption style="text-align: center;"><strong>interval()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#interval-java.time.Duration-">interval()</a></figcaption>
</figure>

```java
    Flux.interval(java.time.Duration.ofMillis(300))
        .take(5)
        .subscribe(System.out::println);
    // Output: 0, 1, 2, 3, 4 (émis avec un délai de 300ms entre chaque élément)
```

11. `zipWith(Publisher<? extends T2> source2)` : combine les éléments de ce Flux (ou Mono) avec ceux d'un autre Publisher, en émettant une nouvelle valeur contenant les deux éléments sous forme de Tuple2.
<figure>
  <img width="600" src="images/zipWith.png" alt="zipWith()" title="zipWith()">
  <figcaption style="text-align: center;"><strong>zipWith()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#zipWith-org.reactivestreams.Publisher-">zipWith()</a></figcaption>
</figure>

```java
    Flux<String> letters = Flux.just("a", "b", "c");
    Flux<Integer> numbers = Flux.just(1, 2, 3);

    letters.zipWith(numbers)
           .subscribe(tuple -> System.out.println("Tuple: " + tuple));
        // Output: Tuple2{t1=a, t2=1}
        //         Tuple2{t1=b, t2=2}
        //         Tuple2{t1=c, t2=3}

    // Avec une fonction de combinaison :
    letters.zipWith(numbers, (l, n) -> l + n)
           .subscribe(System.out::println);
        // Output: a1
        //         b2
        //         c3
```

12. `doOnSubscribe(Consumer<? super Subscription> onSubscribe)` : Permet d'exécuter une action lorsque la souscription a lieu. La `Subscription` est l'objet retourné au `Subscriber` lors de l'appel à `onSubscribe` (de l'interface `Subscriber`).
<figure>
  <img width="600" src="images/doonsubscribe.png" alt="doOnSubscribe()" title="doOnSubscribe()">
  <figcaption style="text-align: center;"><strong>doOnSubscribe()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html#doOnSubscribe-java.util.function.Consumer-">doOnSubscribe()</a></figcaption>
</figure>

```java
    Flux.just("start")
        .doOnSubscribe(sub -> System.out.println("Subscribed!"))
        .subscribe(System.out::println);
```

13. `doOnComplete(Runnable onComplete)` : Permet d'exécuter une action lorsque le `Publisher` se complète avec succès (appel de `onComplete()` du `Subscriber`).
<figure>
  <img width="600" src="images/dooncomplete.png" alt="doOnComplete()" title="doOnComplete()">
  <figcaption style="text-align: center;"><strong>doOnComplete()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#doOnComplete-java.lang.Runnable-">doOnComplete()</a></figcaption>
</figure>

```java
    Mono.just("done")
        .doOnComplete(() -> System.out.println("Mono completed"))
        .subscribe(System.out::println);
```

14. `doOnError(Consumer<? super Throwable> onError)` : Permet d'exécuter une action lorsqu'une erreur se produit (appel de `onError(Throwable t)` du `Subscriber`).
<figure>
  <img width="600" src="images/doonerror.png" alt="doOnError()" title="doOnError()">
  <figcaption style="text-align: center;"><strong>doOnError()</strong> - source: <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#doOnError-java.util.function.Consumer-">doOnError()</a></figcaption>
</figure>

```java
    Flux.error(new RuntimeException("Something went wrong"))
        .doOnError(e -> System.err.println("Error caught!"))
        .subscribe(null, System.err::println);
```

#### 5.1.3 Schedulers.boundedElastic, Schedulers.parallel, subscribeOn & publishOn

Dans **Reactor**, les **Schedulers** sont essentiels pour gérer l'exécution asynchrone et éviter les blocages des threads. Ils définissent sur quels threads les opérations des flux réactifs vont s'exécuter.

* **`Schedulers.boundedElastic()`** : pour les opérations potentiellement bloquantes (comme lire un fichier ou appeler une API synchrone bloquante). Il crée un pool de threads élastique avec une limite.

* **`Schedulers.parallel()`** : Optimisé pour les tâches non bloquantes et gourmandes en CPU (**CPU intensive tasks**) qui peuvent s'exécuter en parallèle efficacement (comme des calculs intensifs). Il utilise un nombre fixe de threads.

Pour utiliser ces schedulers, **Reactor** fournit deux opérateurs clés **`subscribeOn(Scheduler s)`** et **`publishOn(Scheduler s)`**

* `subscribeOn(Scheduler s)` : Indique sur quel `Scheduler` l'ensemble du flux s'exécutera, à partir de la source. Un seul subscribeOn a un effet.
```java
    Mono.fromCallable(() -> "Blocking Task")
        .subscribeOn(Schedulers.boundedElastic()) // L'exécution de la tâche se fera sur un thread boundedElastic
        .map(String::toUpperCase)
        .subscribe(result -> System.out.println("Résultat sur: " + Thread.currentThread().getName() + ", " + result));
```

* `publishOn(Scheduler s)` : indique sur quel `Scheduler` les opérations suivantes dans le flux s'exécuteront. On peut l'utiliser plusieurs fois pour changer le contexte d'exécution à différents points.
```java
    Flux.range(1, 3)
        .map(i -> "Traitement 1 sur: " + Thread.currentThread().getName())
        .publishOn(Schedulers.parallel()) // Le traitement suivant se fera sur un thread parallel
        .map(s -> "Traitement 2 sur: " + Thread.currentThread().getName() + ", " + s.toUpperCase())
        .subscribe(System.out::println);
```


## 6. Spring WebFlux
[Spring WebFlux](https://docs.spring.io/spring-framework/reference/web/webflux.html) est le framework web réactif non bloquant de Spring. **Basé sur Project Reactor**, il permet de construire des applications web réactives et évolutives en tirant parti des principes de la programmation réactive et de l'API Reactive Streams. Contrairement à Spring MVC, qui repose sur un modèle de servlet bloquant, WebFlux est asynchrone et non bloquant par nature.

### 6.1 Architecture et principes
Spring WebFlux, par sa conception réactive, influence profondément son modèle de threading et le cycle de vie des requêtes. Étant donné que le starter Spring Boot WebFlux inclut Netty comme serveur embarqué par défaut, notre exploration de l'architecture et du modèle de threading se focalisera sur cette implémentation.

#### 6.1.1 EventLoop Thread

Un **EventLoop** est un **thread** unique et non bloquant, géré par **Netty**, qui constitue le modèle de **non-blocking IO** utilisé par **Spring WebFlux**. Au lieu d'attendre passivement les opérations I/O, il surveille en permanence plusieurs canaux de socket (**SocketChannels**) à la recherche d'événements. Lorsqu'un événement se produit, l'**EventLoop** délègue le traitement à un handler dans le pipeline de canaux. Cette approche permet à un petit nombre de threads de gérer efficacement un grand nombre de connexions. Il est crucial de ne pas bloquer l'**EventLoop** pour maintenir la réactivité du serveur.

#### 6.1.2 Cycle de vie d'une requete dans Spring Webflux

<figure>
  <img width="600" src="images/spring_webflux_threading_model.png" alt="Spring Webflux Threading Model" title="Spring Webflux Threading Model">
  <figcaption style="text-align: center;"><strong>Spring Webflux Threading Model</strong> - source: <a href="https://singhkaushal.medium.com/spring-webflux-eventloop-vs-thread-per-request-model-a42d07ee8502/">Spring Webflux</a></figcaption>
</figure>

1. **Request -> Socket Channel** : une requête HTTP est reçue par un **Socket Channel** géré par Netty.
2. **Inbound Channel Handler -> EventLoop (Netty thread)** : La requête traverse le **Inbound Channel Handler** et est transmise au thread **EventLoop** de Netty.
3. **EventLoop -> Scheduler/Worker Thread** : Si une opération bloquante est nécessaire, le traitement est délégué à un **Scheduler** (ex: `Schedulers.boundedElastic()`) qui utilise un pool de **Worker Threads**. Pour les opérations non bloquantes, le traitement peut continuer sur l'EventLoop.
4. **Scheduler/Worker Thread -> Spring WebFlux Controller/Service** : Le **Worker Thread** exécute le code potentiellement bloquant.
5. **Scheduler/Worker Thread -> ScheduledTaskQueue** : Une fois l'opération bloquante terminée et le résultat prêt à être envoyé, le Worker Thread place une tâche contenant la réponse dans la `ScheduledTaskQueue`.
6. **EventLoop -> ScheduledTaskQueue** : L'**EventLoop** interroge la `ScheduledTaskQueue` pour les tâches terminées.
7. **EventLoop -> Outbound Channel Handler -> Socket Channel -> Response** : L'**EventLoop** récupère la tâche d****e réponse de la `ScheduledTaskQueue`, la traite via le **Outbound Channel Handler** et l'envoie de manière non bloquante au client via le **Socket Channel**.

Cette approche permet à **Spring WebFlux** de gérer un grand nombre de requêtes simultanées avec un nombre limité de threads.

## 7. Persistance réactive avec R2DBC , Reactor Kafka, WebClient 
Pour qu'une application soit véritablement réactive, il faut que l'ensemble du processus le soit : depuis l'interaction de l'utilisateur avec le serveur jusqu'à l'enregistrement des données sur la base de données et la communication avec d'autres services tels que Kafka. C'est pourquoi des outils réactifs ont été créés, comme R2DBC pour les bases de données et Reactor Kafka (ou Spring Kafka Reactive) pour la messagerie. Ils sont conçus pour fonctionner parfaitement avec des outils réactifs comme Spring WebFlux et Project Reactor, en gardant notre application réactive de bout en bout (end-to-end).

### 7.1 Principes de R2DBC
[R2DBC](https://r2dbc.io/) (Reactive Relational Database Connectivity) est une spécification pour l'accès réactif et non bloquant aux bases de données relationnelles sous la JVM. Son objectif principal est de surmonter les limitations de JDBC dans les environnements réactifs en offrant une API asynchrone et non bloquante.  Lorsque l'on travaille avec PostgreSQL dans un contexte réactif avec Spring WebFlux, l'implémentation de R2DBC pour PostgreSQL, fournie par le projet `io.r2dbc:r2dbc-postgresql`, est le choix privilégié pour maintenir la réactivité de bout en bout.

### 7.2 Intégration avec Spring Data R2DBC
**[Spring Data R2DBC](https://spring.io/projects/spring-data-r2dbc)** simplifie considérablement l'interaction réactive avec **PostgreSQL** (ou d'autres bases de données relationnelles supportées) dans une application **Spring WebFlux**. Il fournit une couche d'abstraction au-dessus de l'API **R2DBC** native.

* Exemple de Code (Spring Data R2DBC avec PostgreSQL) :

```java
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("users")
public class User {
    @Id
    private Integer id;
    private String name;
    private Integer age;

    // Constructeurs, getters et setters
}
```

```java
import org.springframework.data.r2dbc.repository.ReactiveCrudRepository;
import org.springframework.data.r2dbc.repository.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface UserRepository extends ReactiveCrudRepository<User, Integer> {
    Mono<User> findByName(String name);
    Flux<User> findByAgeGreaterThan(int age);

    @Query("SELECT * FROM users WHERE name LIKE $1")
    Flux<User> findByNameLike(String pattern);
}
```

```java
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class UserService {

    private final UserRepository userRepository;

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public Mono<User> getUserByName(String name) {
        return userRepository.findByName(name);
    }

    public Flux<User> getAdultUsers() {
        return userRepository.findByAgeGreaterThan(17);
    }

    public Flux<User> searchUsersByName(String query) {
        return userRepository.findByNameLike("%" + query + "%");
    }

    public Mono<User> createUser(User user) {
        return userRepository.save(user);
    }

    public Mono<Void> deleteUser(Integer id) {
        return userRepository.deleteById(id);
    }

    public Flux<User> getAllUsers() {
        return userRepository.findAll();
    }
}
```

### 7.3 Reactor Kafka
[Reactor Kafka](https://projectreactor.io/docs/kafka/release/reference/) est une librairie réactive qui permet d'intégrer [Apache Kafka](https://kafka.apache.org/) dans des applications basées sur Project Reactor. Elle offre une api non bloquante et basée sur **Reactive Streams** pour interagir avec Kafka, tant pour la production que pour la consommation de messages. Dans une application Spring WebFlux.

### 7.4 Intégration avec Spring Kafka
**Spring Kafka Reactive** est un projet qui fait part de l'écosystème **[Spring Kafka](https://docs.spring.io/spring-kafka/reference/)** qui fournit une API réactive pour interagir avec Apache Kafka en utilisant Project Reactor. Il offre une d'abstraction de **Reactor Kafka**.

**Exemple de Code (Consommation avec ReactiveKafkaConsumerTemplate) :**
```java
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import java.util.HashMap;
import java.util.Map;
import reactor.core.publisher.Flux;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "my-spring-reactive-group");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate(ConsumerFactory<String, String> consumerFactory) {
        return new ReactiveKafkaConsumerTemplate<>(consumerFactory);
    }

    @Bean
    public Flux<String> kafkaConsumerFlux(ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate) {
        return reactiveKafkaConsumerTemplate
                .receiveAutoAck()
                .map(consumerRecord -> consumerRecord.value())
                .doOnNext(message -> System.out.println("Received message: " + message));
    }
}
```

**Exemple de Code (Production avec ReactiveKafkaProducerTemplate) :**
```java
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import java.util.HashMap;
import java.util.Map;
import reactor.core.publisher.Mono;
import org.apache.kafka.clients.producer.ProducerRecord;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate(ProducerFactory<String, String> producerFactory) {
        return new ReactiveKafkaProducerTemplate<>(producerFactory);
    }

    public Mono<Void> sendMessage(ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate, String topic, String key, String payload) {
        return reactiveKafkaProducerTemplate
                .send(new ProducerRecord<>(topic, key, payload))
                .doOnSuccess(senderResult -> System.out.println("Sent message with offset: " + senderResult.recordMetadata().offset()))
                .then();
    }
}
```

## 8. POC pratique

## 8.1 Pré-requis

1.  Java 21 & Maven
2.  Docker & Docker Compose

## 8.2 Lancement du POC

```bash
cd spring-webflux-poc
docker compose up -d
cd common
mvn compile
mvn schema-registry:register
cd ../auth-service
mvn spring-boot:run
cd ../ride-service
mvn spring-boot:run
cd ../driver-service
mvn spring-boot:run
cd ../api-gateway
mvn spring-boot:run
```

## 8.3 Technologies utilisées

1.  [Spring Webflux](https://docs.spring.io/spring-framework/reference/web/webflux.html)
2.  [Spring Security](https://spring.io/projects/spring-security)
3.  [Spring Cloud Gateway Reactive](https://docs.spring.io/spring-cloud-gateway/reference/spring-cloud-gateway.html)
4.  [Apache Kafka](https://kafka.apache.org/)
5.  [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
6.  [Apache Avro](https://avro.apache.org/) comme format de **sérialisation** et **désérialisation** des messages kafka.
7.  [PostgreSQL](https://www.postgresql.org/) et [Spring Data R2DBC]().
8.  [MongoDB](https://www.mongodb.com/) et [Spring Data MongoDB Reactive](https://www.baeldung.com/spring-data-mongodb-reactive).
9.  [Spring for Apache Kafka Reactive](https://spring.io/projects/spring-kafka)

## 8.4 Fonctionnalités

Ce POC est une simple application illustrant la programmation réactive avec **Spring Webflux** et **Reactor**, ainsi que la connexion aux bases de données en mode réactif avec **Spring Data R2DBC** et **Spring Data MongoDB Reactive** et **Spring Kafka**.


* L'utilisateur peut créer un compte en tant que `USER` ou `DRIVER`. Après cela, il s'authentifie et récupère son **token JWT**.

* Un utilisateur possédant le rôle `USER` peut créer des demandes de courses (**Ride Requests**). Il peut également voir en temps réel (implémenté avec SSE - **Server-Sent Events**) les propositions de plusieurs conducteurs (`DRIVER`) et finalement accepter une proposition de course.

* Un utilisateur possédant le rôle `DRIVER` peut voir en temps réel les demandes de courses près de lui (implémenté avec SSE - **Server-Sent Events**) et répondre à une demande par une proposition.

L'application est découpée en trois **micro-services** qui se trouvent derrière une **API Gateway** :

* **auth-service** : Ce micro-service gère l'inscription et l'authentification. Il utilise **PostgreSQL** comme base de données.
* **ride-service** : Ce micro-service permet de créer des demandes de courses, de voir les propositions des conducteurs et d'accepter une proposition. Il utilise **PostgreSQL** comme base de données.
* **driver-service** : Ce micro-service permet de voir les demandes de courses près du conducteur et de proposer des courses. Il utilise **MongoDB** comme base de données.
* **api-gateway** : Point d'entrée de notre POC, elle redirige les requêtes vers le bon micro-service et gère l'autorisation.

-----

* Quand le client (`USER`) crée une demande de course, il envoie une requête à `ride-service` (`/api/rides`). La demande de course est stockée dans la table **PostgreSQL** `ride_requests` avec le statut `REQUESTED`. Ensuite, celle-ci est publiée sur un **topic Kafka** (`poc-ride-request-requested-v1`) et consommée par `driver-service`, qui la stocke dans une collection **MongoDB** `ride_requests`. En temps réel, la requête est affichée aux conducteurs qui se situent près du client.
* Les conducteurs (`DRIVER`) proposent des courses pour cette demande avec des prix, en utilisant l'endpoint `/api/rides/proposals` de `driver-service`. Cette proposition est stockée dans la collection **MongoDB** `ride_proposals` et publiée sur un **topic Kafka** (`poc-ride-request-proposed-v1`). Elle est ensuite consommée par `ride-service`, qui affiche cette proposition au client en temps réel.
* Le client peut maintenant accepter une proposition en passant par l'endpoint `/api/rides/{rideRequestId}/accept` de `ride-service`. La demande de course passe alors au statut `ACCEPTED` et est publiée dans un **topic Kafka** (`poc-ride-request-accepted-v1`). Ce message est consommé par `driver-service`, qui confirme la réservation en mettant le statut de la demande de course à `BOOKED` et notifie `ride-service` que le conducteur est réservé en publiant un message sur le **topic Kafka** (`poc-ride-request-booked-v1`). Finalement, `ride-service` modifie le statut de la demande à `BOOKED`.

## 9. Références
* [Project Reactor](https://projectreactor.io)
* [Spring WebFlux](https://docs.spring.io/spring-framework/reference/web/webflux.html)
* [linux-thread](https://www.scaler.com/topics/linux-thread/)
* [ult-and-klt](https://www.scaler.com/topics/user-level-threads-and-kernel-level-threads/)
* [Types of thread models](https://www.researchgate.net/figure/Three-types-of-thread-models-Popular-operating-systems-5-22-24-adopt-the_fig1_346379550)
* [Webflux vs Thread per request model](https://singhkaushal.medium.com/spring-webflux-eventloop-vs-thread-per-request-model-a42d07ee8502)
* [Concurrency in Spring WebFlux](https://www.baeldung.com/spring-webflux-concurrency)
* [Reactive Manifesto](https://www.reactivemanifesto.org/fr)
* [Flux (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#just-T...-)
* [Subscribe() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#subscribe--)
* [doOnNext() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#doOnNext-java.util.function.Consumer-)
* [map() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#map-java.util.function.Function-)
* [filter() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#filter-java.util.function.Predicate-)
* [flatMap() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#flatMap-java.util.function.Function-)
* [concatMap() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#concatMap-java.util.function.Function-)
* [delayElements() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#delayElements-java.time.Duration-)
* [take() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#take-long-)
* [interval() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#interval-java.time.Duration-)
* [zipWith() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#zipWith-org.reactivestreams.Publisher-)
* [doOnSubscribe() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html#doOnSubscribe-java.util.function.Consumer-)
* [doOnComplete() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#doOnComplete-java.lang.Runnable-)
* [doOnError() (Project Reactor)](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#doOnError-java.util.function.Consumer-)
* [R2DBC](https://r2dbc.io/)
* [Spring Data R2DBC](https://spring.io/projects/spring-data-r2dbc)
* [Reactor Kafka](https://projectreactor.io/docs/kafka/release/reference/)
* [Apache Kafka](https://kafka.apache.org/)
* [Spring Kafka](https://docs.spring.io/spring-kafka/reference/)