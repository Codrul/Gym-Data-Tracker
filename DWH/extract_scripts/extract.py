import extract_exercise_muscles as eem
import extract_exercises as ee
import extract_muscles as em 
import extract_resistance_types as ert 
import extract_workouts as ew 
import connect as c


def main():
    gc = c.connect_sheets()
    engine = c.connect_db()
    ee.load_exercises(gc, engine)
    em.load_muscles(gc, engine)
    eem.load_exercise_muscle(gc, engine)
    ert.load_resistance_types(gc, engine)
    ew.load_workouts(gc, engine)


if __name__ == '__main__':
    main()
