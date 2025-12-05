import extract_exercise_muscles as eem
import extract_exercises as ee
import extract_muscles as em
import extract_resistance_types as ert
import extract_workouts as ew
import connect as c


def main():
    # creating two lists for error and success messages
    error_msg = []
    success_msg = []
    # defining the connection to the db and to the sheet as variables to pass them into worker functions
    gc = c.connect_sheets()
    engine = c.connect_db()

    ee.load_exercises(gc, engine, error_msg, success_msg)
    em.load_muscles(gc, engine, error_msg, success_msg)
    eem.load_exercise_muscle(gc, engine, error_msg, success_msg)
    ert.load_resistance_types(gc, engine, error_msg, success_msg)
    ew.load_workouts(gc, engine, error_msg, success_msg)

    if error_msg:
        for err in error_msg:
            print(err)

    if success_msg:
        for msg in success_msg:
            print(msg)


if __name__ == '__main__':
    main()
