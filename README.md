# Programmation Réactive en Java & SpringBoot

## 1. Introduction
Depuis plusieurs années, le développement web en Java repose sur les servlets, un modèle synchrone et bloquant (thread/request model).  Cependant, ce modèle devient rapidement inefficace dans les applications modernes et les architectures microservices.  Ces dernières nécessitent le traitement d'un grand nombre de requêtes simultanées en temps réel.  
Pour pallier ces contraintes, le modèle réactif a vu le jour, qui se base sur l’asynchronicité et la non-blocage, il permet une utilisation plus efficiente des ressources système.  
Ce POC vise à comparer le modèle traditionnel et l’approche réactive. Il propose une démo basée sur [Project Reactor](https://projectreactor.io) et [Spring WebFlux](https://docs.spring.io/spring-framework/reference/web/webflux.html), en exposant les bases de programmation réactive et la manière d’intégrer ces outils avec des bases de données réactives.

## 2. Threads & Processus
Avant d'entamer la programmation réactive, on va voir comment un système d'exploitation gère les threads d'éxecution.
### 2.1 Processus
Un processus correspond à l'exécution d'un exécutable. Il se compose de :
<figure>
  <img src="images/processus_threads.png" alt="A beautiful sunset" title="Sunset over the ocean">
  <figcaption>Source: <a href="https://www.scaler.com/topics/linux-thread/">Scaler - Topics</a></figcaption>
</figure>

- **Code (Text)**: contient les instructions du programme à exécuter (read-only).
- **Data**: contient les variables globales et statiques initialisées.
- **Heap**: zone mémoire partagée entre les threads pour stocker les objets dynamiques.
- **Files**: descripteurs de fichiers ouverts par le processus (fichiers, sockets réseau, ...).
- **Threads**: unités d'exécution concurrentes au sein du processus, partageant le même espace d'adressage.

Chaque thread possède:

- **Program Counter (PC)**: pointeur vers la prochaine instruction à exécuter
- **Registers**: Registres du processeur contenant les données en cours de traitement.
- **Stack**: Zone mémoire privée contenant les variables locales et paramètres des fonctions.
### 2.2 Threads
il y a 2 types de threads:
- User-level threads: ce sont des threads crées et gérés par des des programmes, librairies par exemple: les threads crées par JVM, les threads crées par le module threading du Python.
- Kernel-level threads: ce sont des threads 
## 3. Modèle traditionnel (servlet)
### 3.1 modèle traditionnel

### 3.2 Modèle réactif

## 4. Reactive Manifesto

## 5. L'initiative Reactive streams

## 6. Programmation reactive en java
### 6.1 Reactor, RxJava, Mutiny, vertx
### 6.2 Définitions (Back pressure, ...)
## 4. Reactor
### 4.1 Reactor core

### 4.2 Reactor netty

### 4.3 Reactor kafka


## 5. Spring Webflux

## 6. R2DBC

## 7. Spring Webflux vs Spring MVC 

## 8. POC (maybe an api using spring webflux + postgres or redis + kafka ??? + htmx ???)

##  9. Bonus: comment la programmation réactive est implémentée (epoll/kqueue syscalls)