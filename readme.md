# Gym Data Tracker

> ## Project Scope
> 
> This project wants to create a data pipeline which extracts data from Google Sheets, passes it through a database for
> storage and uses a dimensional
> layer to create real-time Power BI visuals.



> ## Chapter 1: _Data Modeling_
> The first step was to architecture the Data Warehouse. Before starting 
> to track workouts I needed to think about how 
> I will have my data structured in my DB. 
> I wanted to get straight into writing SQL,
> but I knew it would get messy along the way.
> So I started by creating a conceptual model for my 
> data warehouse.
> 
> **insert conceptual png**
> 
> I thought about what I want to track in my reports and came up with the model above.
> As I have done in my previous DWH projects, I am going with a mix of Inmon and Kimball's approach 
> to data warehousing. At this stage I am thinking whether reporting from a 3NF layer is 
> a bad practice at the scale of my project. Considering the number of my entities, 
> it should not turn into a join complexity issue when generating reports. If I find that to be the issue I will
> add a dimensional layer on top to ease the engine from handling joins, however I will avoid this for now 
> to reduce the redundancy of the ETL pipeline. I do not want to add extra steps and complexity even though usually that
> is the best practice.
> 
> The entities I want to track in my warehouse are
> 1. **Exercises** -- the actual movement being performed. 
> I was thinking of adding another entity named 'exercise_variations' but I do not have much use 
> for grouping exercises into multiple categories at this point.
> 2. **Muscles** -- the actual skeletal muscles being used in the movements exercised. I did not want to go 
> too in depth with muscles as well because it can become very complex without reaping much analytical 
> reward (e.g. knowing your 20 muscles similar to your flexor hallucis are being worked alongside your calves is not 
> essential information for me at this moment).
> 3. **Workouts**  -- the actual training session. I could have inferred this entity by having date_id + set + exercise be a
> primary key, but I thought about the possibility of having multiple workouts in a day so I decided to make this an
> entity.
> 4. **Resistance types** -- the equipment or lack thereof used with the exercises. I wanted this as a separate dimension
> because I do not want it tied to the exercises entity through a bridge since almost any exercise can be modified to 
> use any resistance (this is debatable but not relevant in our context)
> 5. **Date** -- this will be a separate dimension for reporting purposes such as knowing what day of the week it is, which
> day of the month it is, etc.
> 
> Now that we have these in mind, we can start adding attributes and think about how they should be related to 
> eachother. 
> 