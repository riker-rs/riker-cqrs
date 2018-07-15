# Riker Command Query Responsibility Separation (CQRS)

## Overview

Command Query Responsibility Separation (CQRS) builds of top of event sourcing to provide a more structured approach to persistence. Event sourcing alone works well for restoring individual actor state in an actor system with a fixed number of actors. This could be taken further so that data entities can be modelled as actors. For example, an entity could be a User, Account, Post, Transaction, Order, etc, where every instance is represented by its own actor instance.

To make changes to an entity commands are sent to the actor representing that entity. For example, to change the password of a `User` entity an `UpdatePasswordCmd` can be sent, or to disable the user a `DisableUserCmd` can be sent. When an actor receives a command it validates it and then emits an event that will be persisted and applied:

```
UpdatePasswordCmd => PasswordUpdatedEvt
DisableUserCmd => UserDisabledEvt
```

To help with setting up entities and command management Riker CQRS is a separate crate (`riker-cqrs`) that introduces:

- Entity management
- Command based messaging

Since each entity has its own actor there needs to be a coordinator that creates actors when needed and routes commands to the right actor. Basic bookkeeping is also required, so that actors can sleep and be removed from memory after a period of inactivity and then restored when they're needed to handle a command.

Let's look at how to set up an entity manager that represents bank accounts `BankAccount`:

```rust
use riker_cqrs::*;

let model: DefaultModel<TestMsg> = DefaultModel::new();
let sys = ActorSystem::new(&model).unwrap();

let em = Entity::new(&sys,
                    BankAccountProps,
                    "BankAccont",
                    None).unwrap();
```

Here an `Entity` has been created that will manage all instances of bank accounts. It will create new actors if necessary and route commands.

Let's create a new bank account and make a first deposit:

```rust
let number = "12345678";
let name = "Dolores Abernathy";

// create bank account
let cmd = CQMsg::Cmd(number.into(), Protocol::CreateAccountCmd(name.into()));
em.tell(cmd, None);

// deposit $1000
let cmd = CQMsg::Cmd(number.into(), Protocol::DepositCmd(1000));
em.tell(cmd, None);
```

Commands require an ID and based on that ID the entity manager will route the command to the actor for that ID. If there is no currently live actor in memory for that ID the manager will start an actor. Any events associated with that ID will be loaded and the actor state restored before handling the command.

Instead of managing actor creation directly using `actor_of` the entity manager does this instead. You will have noticed that `Entity::new` in the example was passed `BankAccountProps`. This is a struct that implements the `EntityActorProps` trait.

Since each entity actor requires its own unique ID the standard `Props` used in `actor_of` is not sufficient. Instead `EntityActorProps` is implemented:

```rust
struct BankAccountProps;

impl EntityActorProps for BankAccountProps {
    type Msg = Protocol;

    fn props(&self, id: String) -> BoxActorProd<Self::Msg> {
        Props::new_args(Box::new(BankAccountActor::new), id)
    }
}
```
