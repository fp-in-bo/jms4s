// When the user clicks on the search box, we want to toggle the search dropdown
function displayToggleSearch(e) {
  e.preventDefault();
  e.stopPropagation();

  closeDropdownSearch(e);
  
  if (idx === null) {
    console.log("Building search index...");
    prepareIdxAndDocMap();
    console.log("Search index built.");
  }
  const dropdown = document.querySelector("#search-dropdown-content");
  if (dropdown) {
    if (!dropdown.classList.contains("show")) {
      dropdown.classList.add("show");
    }
    document.addEventListener("click", closeDropdownSearch);
    document.addEventListener("keydown", searchOnKeyDown);
    document.addEventListener("keyup", searchOnKeyUp);
  }
}

//We want to prepare the index only after clicking the search bar
var idx = null
const docMap = new Map()

function prepareIdxAndDocMap() {
  const docs = [  
    {
      "title": "Code of conduct",
      "url": "/jms4s/extra_md/code-of-conduct.html",
      "content": "Code of Conduct We are committed to providing a friendly, safe and welcoming environment for all, regardless of level of experience, gender, gender identity and expression, sexual orientation, disability, personal appearance, body size, race, ethnicity, age, religion, nationality, or other such characteristics. Everyone is expected to follow the Scala Code of Conduct when discussing the project on the available communication channels. If you are being harassed, please contact us immediately so that we can support you. Moderation Any questions, concerns, or moderation requests please contact a member of the project."
    } ,      
    {
      "title": "Transacted Consumer",
      "url": "/jms4s/programs/tx-consumer/",
      "content": "Transacted Consumer A JmsTransactedConsumer is a consumer that will use a local transaction to receive a message and which lets the client decide whether to commit or rollback it. Its only operation is: def handle(f: (JmsMessage, MessageFactory[F]) =&gt; F[TransactionAction[F]]): F[Unit] This is where the user of the API can specify its business logic, which can be any effectful operation. Creating a message is as effectful operation as well, and the MessageFactory argument will provide the only way in which a client can create a brand new message. This argument can be ignored if the client is only consuming messages. What handle expects is a TransactionAction[F], which can be either: a TransactionAction.commit, which will instructs the lib to commit the local transaction a TransactionAction.rollback, which will instructs the lib to rollback the local transaction a TransactionAction.send in all its forms, which can be used to send 1 or multiple messages to 1 or multiple destinations and then commit the local transaction The consumer can be configured specifying a concurrencyLevel, which is used internally to scale the operations (receive and then process up to concurrencyLevel). A complete example is available in the example project."
    } ,    
    {
      "title": "Auto-Acknowledger Consumer",
      "url": "/jms4s/programs/auto-ack-consumer/",
      "content": "Auto Acknowledger Consumer A JmsAutoAcknowledgerConsumer is a consumer that will automatically acknowledge a message after its reception. Its only operation is: def handle(f: (JmsMessage, MessageFactory[F]) =&gt; F[AutoAckAction[F]]): F[Unit] This is where the user of the API can specify its business logic, which can be any effectful operation. Creating a message is as effectful operation as well, and the MessageFactory argument will provide the only way in which a client can create a brand new message. This argument can be ignored if the client is only consuming messages. What handle expects is an AutoAckAction[F], which can be either: an AckAction.noOp, which will instructs the lib to do nothing since the message will be acknowledged regardless an AckAction.send in all its forms, which can be used to send 1 or multiple messages to 1 or multiple destinations The consumer can be configured specifying a concurrencyLevel, which is used internally to scale the operations (receive and then process up to concurrencyLevel). A complete example is available in the example project."
    } ,    
    {
      "title": "Acknowledger Consumer",
      "url": "/jms4s/programs/ack-consumer/",
      "content": "Acknowledger Consumer A JmsAcknowledgerConsumer is a consumer which let the client decide whether confirm (a.k.a. ack) or reject (a.k.a. nack) a message after its reception. Its only operation is: def handle(f: (JmsMessage, MessageFactory[F]) =&gt; F[AckAction[F]]): F[Unit] This is where the user of the API can specify its business logic, which can be any effectful operation. Creating a message is as effectful operation as well, and the MessageFactory argument will provide the only way in which a client can create a brand new message. This argument can be ignored if the client is only consuming messages. What handle expects is an AckAction[F], which can be either: an AckAction.ack, which will instructs the lib to confirm the message an AckAction.noAck, which will instructs the lib to do nothing an AckAction.send in all its forms, which can be used to instruct the lib to send 1 or multiple messages to 1 or multiple destinations The consumer can be configured specifying a concurrencyLevel, which is used internally to scale the operations (receive and then process up to concurrencyLevel). A complete example is available in the example project."
    } ,    
    {
      "title": "Producer",
      "url": "/jms4s/programs/producer/",
      "content": "Producer A JmsProducer is a producer that lets the client publish a message in queues/topics. sendN: to send N messages to N Destinations. def sendN( makeN: MessageFactory[F] =&gt; F[NonEmptyList[(JmsMessage, DestinationName)]] ): F[Unit] sendNWithDelay: to send N messages to N Destinations with an optional delay. def sendNWithDelay( makeNWithDelay: MessageFactory[F] =&gt; F[NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]] ): F[Unit] sendWithDelay: to send a message to a Destination. def sendWithDelay( make1WithDelay: MessageFactory[F] =&gt; F[(JmsMessage, (DestinationName, Option[FiniteDuration]))] ): F[Unit] send: to send a message to a Destination. def send( make1: MessageFactory[F] =&gt; F[(JmsMessage, DestinationName)] ): F[Unit] For each operation, the client has to provide a function that knows how to build a JmsMessage given a MessageFactory. This may appear counter-intuitive at first, but the reason behind this design is that creating a JmsMessage is an operation that involves interacting with JMS APIs, and we want to provide a high-level API so that the user can’t do things wrong. A complete example is available in the example project. A note on concurrency A JmsProducer can be used concurrently, performing up to concurrencyLevel concurrent operation."
    } ,    
    {
      "title": "Program",
      "url": "/jms4s/programs/",
      "content": "Program A program is a high level interface that will hide all the interaction with low level JMS apis. This library is targetting JMS 2.0. There are different types of programs for consuming and/or producing, each of them is parameterized on the effect type (eg. IO). The data they consume/produce is kept as-is, thus the de/serialization is left to the user of the api (e.g. json, xml, text). Currently only javax.jms.TextMessage is supported, but we designed the api in order to easily support new message types as soon as we see demand or contributions. JmsTransactedConsumer: A consumer that supports local transactions, also producing messages to destinations. JmsAcknowledgerConsumer: A consumer that leaves the choice to acknowledge (or not to) message consumption to the user, also producing messages to destinations. JmsAutoAcknowledgerConsumer: A consumer that acknowledges message consumption automatically, also producing messages to destinations. JmsProducer: Producing messages to one or more destinations. Concurrency with JMS? The core of all JMS is the entity named JMSContext, introduced with JMS 2.0. The only safe way to scale things a bit is to create a “root” context (which will open a physical connection) and from that creating multiple other contexts. This library will keep a pool of contexts so that the user can scale up to pre-defined concurrency level."
    } ,    
    {
      "title": "IBM MQ",
      "url": "/jms4s/providers/ibm-mq/",
      "content": "IBM MQ Creating a jms client for an IBM MQ queue manager is as easy as: import cats.data.NonEmptyList import cats.effect.{ IO, Resource } import jms4s.ibmmq.ibmMQ import jms4s.ibmmq.ibmMQ._ import jms4s.JmsClient import org.typelevel.log4cats.Logger def jmsClientResource(implicit L: Logger[IO]): Resource[IO, JmsClient[IO]] = ibmMQ.makeJmsClient[IO]( Config( qm = QueueManager(\"YOUR.QM\"), endpoints = NonEmptyList.one(Endpoint(\"localhost\", 1414)), channel = Channel(\"YOUR.CHANNEL\"), username = Some(Username(\"YOU\")), password = Some(Password(\"PW\")), clientId = ClientId(\"YOUR.APP\") ) )"
    } ,    
    {
      "title": "Active MQ Artemis",
      "url": "/jms4s/providers/active-mq-artemis/",
      "content": "Active MQ Artemis Creating a jms client for an Active MQ Artemis cluster is as easy as: import cats.data.NonEmptyList import cats.effect.{ IO, Resource } import jms4s.activemq.activeMQ import jms4s.activemq.activeMQ._ import org.typelevel.log4cats.Logger import jms4s.JmsClient def jmsClientResource(implicit L: Logger[IO]): Resource[IO, JmsClient[IO]] = activeMQ.makeJmsClient[IO]( Config( endpoints = NonEmptyList.one(Endpoint(\"localhost\", 61616)), username = Some(Username(\"YOU\")), password = Some(Password(\"PW\")), clientId = ClientId(\"YOUR.APP\") ) ) Why not ActiveMQ 5 “Classic”? ActiveMQ 5 “Classic” is only supporting JMS 1.1, which is missing a bunch of features we really need to offer."
    } ,    
    {
      "title": "Providers",
      "url": "/jms4s/providers/",
      "content": "Providers Currently supported: Active MQ Artemis IBM MQ Supporting a new provider is a task which should be pretty straightforward. I you need a provider which is missing you can: Try contributing! PRs are always welcome! Raise an issue. Let us know, someone may eventually pick that up or we can guide you to a complete PR."
    } ,    
    {
      "title": "License",
      "url": "/jms4s/extra_md/license.html",
      "content": "Copyright 2020 Functional Programming in Bologna Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software. THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE."
    } ,        
  ];

  idx = lunr(function () {
    this.ref("title");
    this.field("content");

    docs.forEach(function (doc) {
      this.add(doc);
    }, this);
  });

  docs.forEach(function (doc) {
    docMap.set(doc.title, doc.url);
  });
}

// The onkeypress handler for search functionality
function searchOnKeyDown(e) {
  const keyCode = e.keyCode;
  const parent = e.target.parentElement;
  const isSearchBar = e.target.id === "search-bar";
  const isSearchResult = parent ? parent.id.startsWith("result-") : false;
  const isSearchBarOrResult = isSearchBar || isSearchResult;

  if (keyCode === 40 && isSearchBarOrResult) {
    // On 'down', try to navigate down the search results
    e.preventDefault();
    e.stopPropagation();
    selectDown(e);
  } else if (keyCode === 38 && isSearchBarOrResult) {
    // On 'up', try to navigate up the search results
    e.preventDefault();
    e.stopPropagation();
    selectUp(e);
  } else if (keyCode === 27 && isSearchBarOrResult) {
    // On 'ESC', close the search dropdown
    e.preventDefault();
    e.stopPropagation();
    closeDropdownSearch(e);
  }
}

// Search is only done on key-up so that the search terms are properly propagated
function searchOnKeyUp(e) {
  // Filter out up, down, esc keys
  const keyCode = e.keyCode;
  const cannotBe = [40, 38, 27];
  const isSearchBar = e.target.id === "search-bar";
  const keyIsNotWrong = !cannotBe.includes(keyCode);
  if (isSearchBar && keyIsNotWrong) {
    // Try to run a search
    runSearch(e);
  }
}

// Move the cursor up the search list
function selectUp(e) {
  if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index) && (index > 0)) {
      const nextIndexStr = "result-" + (index - 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Move the cursor down the search list
function selectDown(e) {
  if (e.target.id === "search-bar") {
    const firstResult = document.querySelector("li[id$='result-0']");
    if (firstResult) {
      firstResult.firstChild.focus();
    }
  } else if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index)) {
      const nextIndexStr = "result-" + (index + 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Search for whatever the user has typed so far
function runSearch(e) {
  if (e.target.value === "") {
    // On empty string, remove all search results
    // Otherwise this may show all results as everything is a "match"
    applySearchResults([]);
  } else {
    const tokens = e.target.value.split(" ");
    const moddedTokens = tokens.map(function (token) {
      // "*" + token + "*"
      return token;
    })
    const searchTerm = moddedTokens.join(" ");
    const searchResults = idx.search(searchTerm);
    const mapResults = searchResults.map(function (result) {
      const resultUrl = docMap.get(result.ref);
      return { name: result.ref, url: resultUrl };
    })

    applySearchResults(mapResults);
  }

}

// After a search, modify the search dropdown to contain the search results
function applySearchResults(results) {
  const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
  if (dropdown) {
    //Remove each child
    while (dropdown.firstChild) {
      dropdown.removeChild(dropdown.firstChild);
    }

    //Add each result as an element in the list
    results.forEach(function (result, i) {
      const elem = document.createElement("li");
      elem.setAttribute("class", "dropdown-item");
      elem.setAttribute("id", "result-" + i);

      const elemLink = document.createElement("a");
      elemLink.setAttribute("title", result.name);
      elemLink.setAttribute("href", result.url);
      elemLink.setAttribute("class", "dropdown-item-link");

      const elemLinkText = document.createElement("span");
      elemLinkText.setAttribute("class", "dropdown-item-link-text");
      elemLinkText.innerHTML = result.name;

      elemLink.appendChild(elemLinkText);
      elem.appendChild(elemLink);
      dropdown.appendChild(elem);
    });
  }
}

// Close the dropdown if the user clicks (only) outside of it
function closeDropdownSearch(e) {
  // Check if where we're clicking is the search dropdown
  if (e.target.id !== "search-bar") {
    const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
    if (dropdown) {
      dropdown.classList.remove("show");
      document.documentElement.removeEventListener("click", closeDropdownSearch);
    }
  }
}
