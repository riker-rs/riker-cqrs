#[macro_use]
extern crate log;

use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{SystemTime, Duration};

use config::Config;
use riker::actors::*;

pub trait EntityActorProps : Clone + Send + Sync {
    type Msg: Message;
    
    fn props(&self, id: String) -> BoxActorProd<Self::Msg>;
}

impl<Msg, T> EntityActorProps for Arc<Mutex<T>>
    where Msg: Message, T: EntityActorProps<Msg=Msg>
{
    type Msg = Msg;

    fn props(&self, id: String) -> BoxActorProd<Self::Msg> {
        self.lock().unwrap().props(id)
    }
}

pub struct Entity;

impl Entity {
    pub fn new<Pro, Msg>(sys: &ActorSystem<Msg>,
                    instance_fact: Pro,
                    name: &str,
                    conf: Option<EntityActorConfig>) -> Result<ActorRef<Msg>, CreateError>
        where Pro: EntityActorProps<Msg=Msg> + 'static, Msg: Message
    {
        let conf = conf.unwrap_or(EntityActorConfig::from(&sys.config()));
        let props = EntityActor::props(name, instance_fact, conf);
        let actor = sys.actor_of(props, &format!("entity-{}", name))?;

        Ok(actor)
    }   
}

struct EntityActor<Pro, Msg: Message> {
    name: String,
    props: Pro,
    instances: HashMap<String, EntityInstance<Msg>>,
    sleep_after: Duration,
}

impl<Pro, Msg> EntityActor<Pro, Msg>
    where Pro: EntityActorProps<Msg=Msg> + 'static, Msg: Message
{
    fn props(name: &str,
            instance_fact: Pro,
            conf: EntityActorConfig) -> BoxActorProd<Msg> {
        Props::new_args(
            Box::new(Self::actor),
            (name.into(),
            instance_fact, conf)
        )
    }

    fn actor((name, instance_fact, conf): (String, Pro, EntityActorConfig)) -> BoxActor<Msg> {
        let actor = EntityActor {
            name,
            props: instance_fact,
            instances: HashMap::new(),
            sleep_after: Duration::from_secs(conf.sleep_after_secs)
        };
        Box::new(actor)
    }

    fn handle_cmd(&mut self,
                    ctx: &Context<Msg>,
                    id: String,
                    cmd: Msg,
                    sender: Option<ActorRef<Msg>>) {

        if self.instances.contains_key(&id) {
            trace!("CQRS: Entity: {}, ID: {}, CMD: {:?}, State: running", self.name, id, cmd);
            let entity = self.instances.get_mut(&id).unwrap();
            entity.actor.tell(cmd, sender);
            entity.last_used = SystemTime::now();
        } else {
            trace!("CQRS: Entity: {}, ID: {}, CMD: {:?}, State: asleep", self.name, id, cmd);
            let entity = ctx.actor_of(self.props.props(id.clone()), id.as_ref()).unwrap();
            entity.tell(cmd, sender);

            let entity = EntityInstance {
                actor: entity,
                last_used: SystemTime::now()
            };
            self.instances.insert(id, entity);
        }
    }

    fn schedule_tick(ctx: &Context<Msg>) {
        ctx.schedule_once(Duration::from_secs(60),
                            ctx.myself(),
                            None,
                            ActorMsg::Tick);
    }

    fn sleep_instances(&mut self, ctx: &Context<Msg>) {
        let count = self.instances.len(); 
        let threshhold = SystemTime::now() - self.sleep_after;

        let (stop, keep): (Vec<(String, EntityInstance<Msg>)>, Vec<(String, EntityInstance<Msg>)>) =
            self.instances
                .drain()
                .partition(|&(_, ref instance)| threshhold > instance.last_used);

        // stop instances
        for instance in stop.into_iter() {
            ctx.stop(&instance.1.actor);
        }

        // keep instances that are not due to sleep
        for instance in keep.into_iter() {
            self.instances.insert(instance.0, instance.1);
        }

        trace!("CQRS: Number of instances put to sleep: {}", count - self.instances.len());
    }
}

impl<Pro, Msg> Actor for EntityActor<Pro, Msg>
    where Pro: EntityActorProps<Msg=Msg> + 'static, Msg: Message
{
    type Msg = Msg;

    fn pre_start(&mut self, ctx: &Context<Msg>) {
        Self::schedule_tick(ctx);
    }

    fn receive(&mut self, _: &Context<Msg>, _: Msg, _: Option<ActorRef<Msg>>) {}

    fn other_receive(&mut self,
                    ctx: &Context<Msg>,
                    msg: ActorMsg<Msg>,
                    sender: Option<ActorRef<Msg>>) {
        match msg {
            ActorMsg::CQ(cq) => {
                match cq {
                    CQMsg::Cmd(id, cmd) => self.handle_cmd(ctx, id, cmd, sender),
                } 
            }
            ActorMsg::Tick => {
                self.sleep_instances(ctx);
                Self::schedule_tick(ctx);
            }
            _ => {}
        }
    }
}

struct EntityInstance<Msg: Message> {
    last_used: SystemTime,
    actor: ActorRef<Msg>,
}

#[derive(Clone, Debug)]
pub struct EntityActorConfig {
    sleep_after_secs: u64,
}

impl<'a> From<&'a Config> for EntityActorConfig {
    fn from(config: &Config) -> Self {
        EntityActorConfig {
            sleep_after_secs: config.get_int("cqrs.sleep_after_secs").unwrap() as u64
        }
    }
}


#[cfg(test)]
mod tests {
    use std::{thread, time};
    use riker::actors::*;
    use riker_default::DefaultModel;

    use crate::{Entity, EntityActorProps};

    #[derive(Clone, Debug)]
    pub enum TestMsg {
        CreateAccountCmd(String),
        AddAmountCmd(i32),

        AccountCreatedEvt(BankAccount),
        AmountAddedEvt(i32),
    }

    impl Into<ActorMsg<TestMsg>> for TestMsg {
        fn into(self) -> ActorMsg<TestMsg> {
            ActorMsg::User(self)
        }
    }

    #[derive(Clone, Debug)]
    pub struct BankAccount {
        id: String,
        name: String,
        balance: i32,
    }

    pub struct BankAccountActor {
        id: String,
        state: Option<BankAccount>
    }

    impl BankAccountActor {
        pub fn new(id: String) -> BoxActor<TestMsg> {
            let actor = BankAccountActor {
                id: id,
                state: None
            };

            Box::new(actor)
        }

        fn create_account(&mut self, ctx: &Context<TestMsg>, cmd: TestMsg) {
            match cmd {
                TestMsg::CreateAccountCmd(name) => {
                    let account = BankAccount {
                        id: self.id.clone(),
                        name: name,
                        balance: 0
                    };

                    ctx.persist_event(TestMsg::AccountCreatedEvt(account));
                }
                _ => {
                    println!("Can't update a non-existing account");
                }
            }
        }

        fn update_account(&mut self, ctx: &Context<TestMsg>, cmd: TestMsg) {
            match cmd {
                TestMsg::AddAmountCmd(amount) => self.add_amount(ctx, amount),
                _ => {}
            }
        }

        fn add_amount(&mut self, ctx: &Context<TestMsg>, amount: i32) {
            match self.state {
                Some(ref account) => {
                    println!("Current balance {}", account.balance);
                    ctx.persist_event(TestMsg::AmountAddedEvt(amount));
                }
                None => {}
            }
        }
    }

    impl Actor for BankAccountActor {
        type Msg = TestMsg;
        
        fn receive(&mut self,
                    ctx: &Context<TestMsg>,
                    msg: TestMsg,
                    _: Option<ActorRef<TestMsg>>) {
            match self.state {
                Some(_) => self.update_account(ctx, msg),
                None => self.create_account(ctx, msg)
            }
        }

        fn apply_event(&mut self, _: &Context<Self::Msg>, evt: Self::Msg) {
            println!("apply event {:?}", evt);
            match evt {
                TestMsg::AccountCreatedEvt(account) => self.state = Some(account),
                TestMsg::AmountAddedEvt(amount) => self.state.as_mut().unwrap().balance += amount,
                _ => {}
            }
        }

        fn persistence_conf(&self) -> Option<PersistenceConf> {
            Some(PersistenceConf {
                id: self.id.clone(),
                keyspace: "persist_test".to_string()
            })
        }
    }

    #[derive(Clone)]
    pub struct BankAccountActorFact;

    impl EntityActorProps for BankAccountActorFact {
        type Msg = TestMsg;

        fn props(&self, id: String) -> BoxActorProd<Self::Msg> {
            Props::new_args(Box::new(BankAccountActor::new), id)
        }
    } 

    #[test]
    fn cqrs() {
        let model: DefaultModel<TestMsg> = DefaultModel::new();
        let system = ActorSystem::new(&model).unwrap();
        
        let em = Entity::new(&system,
                            BankAccountActorFact,
                            "BankAccont",
                            None).unwrap();

        let number = "12345678".to_string();
        let name = "Alex Kamal".to_string();
        em.tell(CQMsg::Cmd(number.clone(), TestMsg::CreateAccountCmd(name)), None);

        let number2 = "87654321".to_string();
        let name2 = "Jules-Pierre Mao".to_string();
        em.tell(CQMsg::Cmd(number2.clone(), TestMsg::CreateAccountCmd(name2)), None);

        let number3 = "65555555".to_string();
        let name3 = "J. Miller".to_string();
        em.tell(CQMsg::Cmd(number3.clone(), TestMsg::CreateAccountCmd(name3)), None);


        em.tell(CQMsg::Cmd(number.clone(), TestMsg::AddAmountCmd(100)), None);
        em.tell(CQMsg::Cmd(number.clone(), TestMsg::AddAmountCmd(100)), None);
        em.tell(CQMsg::Cmd(number, TestMsg::AddAmountCmd(100)), None);

        em.tell(CQMsg::Cmd(number2.clone(), TestMsg::AddAmountCmd(50)), None);
        em.tell(CQMsg::Cmd(number2.clone(), TestMsg::AddAmountCmd(50)), None);
        em.tell(CQMsg::Cmd(number2.clone(), TestMsg::AddAmountCmd(50)), None);

        em.tell(CQMsg::Cmd(number3.clone(), TestMsg::AddAmountCmd(50)), None);
        em.tell(CQMsg::Cmd(number3.clone(), TestMsg::AddAmountCmd(50)), None);
        em.tell(CQMsg::Cmd(number3.clone(), TestMsg::AddAmountCmd(50)), None);

        thread::sleep(time::Duration::from_secs(2));
        system.print_tree();
    }
}
