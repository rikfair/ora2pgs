Tutorial
========

The Three Modules
-----------------

*undated* consists of three main modules, ``undated``, ``undated.fmts`` and ``undated.utils``.
These are distinctly separated for performance, to limit import overhead.

**undated** 

The undated module, is the main module for manipulating dates, it consists of a main class object for managing a date as an integer
and several functions for further functionality. All of these are hopefully self-explanatory. 

**undated.fmts**

The ``fmts`` (formattings) module comes into play when the date data format is unknown.
It has two key class objects. The ``Deriver`` class derives the date format from a list
of dates, returning an ``UndatedFormat`` class object, which contains the information
required by the ``as_parts`` function to extract the date elements.

**undated.utils**

The utils module is tuned for performance. It has little to no validation on the parameters
and works solely with dates stored as integers in the ``Ymd`` format

.. important::
   The tutorial assumes that the `undated`, `undated.fmts`, and `undated.utils` packages have been imported as `ud`, `udf` and `udu` respectfully.

   .. code-block::

      import undated as ud
      import undated.fmts as udf
      import undated.utils as udu

Limitations
-----------

``undated`` was designed with data processing of recent (-ish) dates in mind. Because of this it uses the Gregorian calendar
and makes no adjustment for the Julian calendar changeover. Thus, it will treat any dates before 1583 as invalid.

Common Parameter Names
----------------------

Parameter names have been standardised as much as possible, to be more consistent, intuitive and understandable.

- **day**: the day of the month as an integer
- **fmt**: the date format string or can sometimes be the udf.UndatedFormat object
- **idate**: a date integer of no specific format
- **iymd**: an integer in the year month day format
- **month**: the month as an integer, January == 1
- **sdate**: a date string or integer of no specific format 
- **year**: the year as an integer
- **ymd**: a ud.YMD class object
- **yy_pivot**: for processing two-digit years, the lower bound for converting two-digit years to four-digit years

undated YMD Class
-----------------

The class ``ud.YMD`` is the go-to tool, when manipulation of the dates is required during processing.

.. code-block:: python3

   import undated as ud
   ymd = ud.YMD(2022_07_04)
   print('ymd =', ymd)
   print('plus 7 days =', ymd + 7)
   print('minus 7 days =', ymd - 7)
   print('add 2 months =', ymd.add_months(2))
   print('minus 2 months =', ymd.add_months(-2))
   print('add 10 weekdays =', ymd.add_weekdays(10))
   print('minus 10 weekdays =', ymd.add_weekdays(-10))
   print('add 3 years =', ymd.add_years(3))
   print('minus 3 years =', ymd.add_years(-3))
   print('day of the week =', ymd.day_of_week())
   print('is leap year =', ymd.is_leap_year())
   print('is weekday =', ymd.is_weekday())
   print('the day, month and year =', ymd.day, ymd.month, ymd.year)

gives the results

.. code-block:: text

   ymd = 20220704
   plus 7 days = 20220711
   minus 7 days = 20220627
   add 2 months = 20220904
   minus 2 months = 20220504
   add 10 weekdays = 20220718
   minus 10 weekdays = 20220620
   add 3 years = 20250704
   minus 3 years = 20190704
   day of the week = 1
   is leap year = False
   is weekday = True
   the day, month and year = 4 7 2022


undated Functions
-----------------

The ``add_days``, ``add_months`` and ``add_years`` functions offer an alternative approach to the same functionality as the ``YMD`` class methods.
With the YMD class being passed as a parameter.

.. code-block:: python3

   import undated as ud
   ymd = ud.YMD(2022_07_04)
   print('ymd =', ymd)
   print('plus 7 days =', ud.add_days(ymd, 7))
   print('minus 7 days =', ud.add_days(ymd, -7))
   print('add 2 months =', ud.add_months(ymd, 2))
   print('minus 2 months =', ud.add_months(ymd, -2))
   print('add 10 weekdays =', ud.add_weekdays(ymd, 10))
   print('minus 10 weekdays =', ud.add_weekdays(ymd, -10))

gives the results

.. code-block:: text

   ymd = 20220704
   plus 7 days = 20220711
   minus 7 days = 20220627
   add 2 months = 20220904
   minus 2 months = 20220504
   add 10 weekdays = 20220718
   minus 10 weekdays = 20220620

The ``undated`` module also contains several *between* functions, that accept two YMD class objects.
These calculate the days between two dates, the complete months between two dates or the weekdays, Monday to Friday, between two dates.

.. code-block:: python3

   import undated as ud
   ymd1 = ud.YMD(2022_07_04)
   ymd2 = ud.YMD(2024_05_30)
   print('ymd1 ymd2 =', ymd1, ymd2)
   print('days between =', ud.days_between(ymd1, ymd2))
   print('months between =', ud.months_between(ymd1, ymd2))
   print('weekdays between =', ud.weekdays_between(ymd1, ymd2))

gives the results

.. code-block:: text

   ymd1 ymd2 = 20220704 20240530
   days between = 696
   months between = 22
   weekdays between = 498

Format Deriver
--------------

The ``udf.Deriver`` class analyses the date data and tries to derive the format.
It's designed with large amounts of data in mind, coming from various sources.
It loops through the data until it finds a date that can be of only one format.

.. note::

   The deriver has been designed to solve the problem where different data sources provide dates in different formats.
   The deriver assumes that all dates from the same data source, IE those passed to its search method, are all in the same format.

The following examples uses the `tutorial.csv file on GitHub <https://github.com/rikfair/undated/blob/main/docs/tutorial.csv>`_.
Each date column contains dates in different formats, to represent the different data files being received.

In this example, the deriver is passed a column of date data, ``date1`` in this case, to derive. 

.. code-block:: python3

   import csv
   import undated as ud
   import undated.fmts as udf

   with open('C:/Git/undated/docs/tutorial.csv', newline='') as csvfile:
       data = list(csv.DictReader(csvfile))
       dates = [row['date1'] for row in data]         # Get the required dates into a list
       deriver = udf.Deriver()                        # Initiate the deriver class
       deriver.set_parameters({udf.LANGUAGES: 'EN'})  # Set language to English, only required for date3
       fmt = deriver.search(dates)                    # Search for the date format
       if fmt:
           for ymd in [ud.YMD(udf.as_parts(date, fmt)) for date in dates]: 
               print(ymd)                             # The date is now an integer in Ymd format
       else:
           print('Format not derived')

which gives the results

.. code-block:: text

   20200204
   20210525
   20220831
   20080423
   20060502
   20200229
   None
   20211120
   20201013
   20210104

Changing the ``date1`` column to ``date2`` or ``date3`` will give the same output, as ``udf.Deriver`` will evaluate the correct date format.

.. tip:: 
   Only pass enough dates to the search to be sure of getting a match.
   If there're thousands of rows, a few dozen may be enough to determine the format, and there's always the option of further searches. 

Further Date Formats
--------------------

The ``Deriver`` will try and derive the format from most common date presentations.
The code below is definitely not how the package has been designed to be used but it does show the various date formats that can be accepted.

.. code-block:: python3

   import undated as ud
   import undated.fmts as udf


   def go(lists_of_dates):
       for dates in lists_of_dates:
           fmt = udf.Deriver().search(dates)
           for sdate in dates:
               ymd_parts = udf.as_parts(sdate, fmt)
               print(ud.YMD(ymd_parts) if ymd_parts else 'Error', sdate, sep=' <- ')


   go((
       ('20-mar-20', '21-apr-20', '22-may-20'),
       ('20mar20', '21apr20', '22may20'),
       ('11/25/2020 7:00PM Europe/Berlin',),
       ('25.11.2020 7:00PM Europe/Berlin',),
       ('Monday, 24 May 2021 05:50', 'Monday, 27 June 2021 05:50'),
       ('Mon, 25 Jan 2021 05:50:06 GMT', 'Mon, 27 Dec 2021 05:50:06 GMT'),
       ('Mon, 25 Jan 2021 05:50:06 GMT', 'Mon, 27 Dec 2021 05:50:06 GMT'),
       ('Mon, 25 Ene 2021 05:50:06 CET', 'Mon, 27 Dic 2021 05:50:06 CET'),
       ('12092022', '13092022'),
       ('2021-03-27T05:50:06.7199222-04:00',),
       ('03/28/2021 05:50:06',),
       ('29MAR2020', '01JAN2020'),
       ('Monday, 29 March 2021',),
       ('Monday, 29 March 2021 05:50 AM',),
       ('Monday, 29 March 2021 05:50:06',),
   ))

gives the results

.. code-block::

   20200320 <- 20-mar-20
   20200421 <- 21-apr-20
   20200522 <- 22-may-20
   20200320 <- 20mar20
   20200421 <- 21apr20
   20200522 <- 22may20
   20201125 <- 11/25/2020 7:00PM Europe/Berlin
   20201125 <- 25.11.2020 7:00PM Europe/Berlin
   20210529 <- Monday, 29 May 2021 05:50
   20210629 <- Monday, 29 June 2021 05:50
   20210129 <- Mon, 29 Jan 2021 05:50:06 GMT
   20211229 <- Mon, 29 Dec 2021 05:50:06 GMT
   20210129 <- Mon, 29 Jan 2021 05:50:06 GMT
   20211229 <- Mon, 29 Dec 2021 05:50:06 GMT
   20210129 <- Mon, 29 Ene 2021 05:50:06 CET
   20211229 <- Mon, 29 Dic 2021 05:50:06 CET
   20220912 <- 12092022
   20220913 <- 13092022
   20210327 <- 2021-03-27T05:50:06.7199222-04:00
   20210328 <- 03/28/2021 05:50:06
   20200329 <- 29MAR2020
   20200101 <- 01JAN2020
   20210329 <- Monday, 29 March 2021
   20210329 <- Monday, 29 March 2021 05:50 AM
   20210329 <- Monday, 29 March 2021 05:50:06

Month Languages
---------------

The observant may have spotted some Spanish months in the last example.
The ``Deriver`` currently caters for English, French, German and Spanish, full and abbreviated months names.
If you know the language being used, setting it using the ``set_parameters`` method can improve performance.
Which leads us on to...

Deriver set_parameters
----------------------

To improve performance and assist with the format deriving process, the ``Deriver`` class object can have parameters set.

Hints
^^^^^

Hints help the ``Deriver``, especially when there are fewer dates to use to derive the format.
Current hints are:

- ``udf.Y2`` the year is two-digits
- ``udf.YFIRST`` the year is in the first position
- ``udf.YLAST`` the year is in the last position
- ``udf.YM`` the date only includes the year and month

The following code applies the hints for two-digit years, and the year in the last position.

.. code-block:: python3

   import undated.fmts as udf
   my_date = '200122'
   deriver = udf.Deriver()
   deriver.set_parameters({udf.HINTS: [udf.Y2, udf.YLAST]})
   fmt = deriver.search(my_date)
   print(udf.as_parts(my_date, fmt))

gives the result

.. code-block:: text

   (2022, 1, 20)


Languages
^^^^^^^^^

If dates have text based months, the language can be set if it is known. This will improve performance and accuracy.

.. code-block:: python3

   import undated.fmts as udf
   my_date = '20-JAN-2022'
   deriver = udf.Deriver()
   deriver.set_parameters({udf.LANGUAGES: 'EN1'})
   fmt = deriver.search(my_date)
   print(udf.as_parts(my_date, fmt))
   
gives the result

.. code-block:: text

   (2022, 1, 20)

In the above case, the format would not be derivable without specifying the language, as JAN could be English or German.

The language parameter above is ``EN1``. The ``EN`` refers to the language,
other valid options are ``DE`` German, ``ES`` Spanish and ``FR`` French.
The ``1`` indicates that we are using the abbreviated months, ``2`` being for full month names.
For example, ``ES2`` would be full Spanish month names, ``FR1`` would be abbreviated French months.

Time Separator
^^^^^^^^^^^^^^

Often date strings include the time, which is out of scope for the ``undated`` package, so this needs to be removed.
The default time separator character is ``T``, following ISO standards. Space is also used.
If dates have another separator character, this can be specified. In this example the ``@`` symbol has been used.

.. code-block:: python3

   import undated.fmts as udf
   my_date = '20-02-2022@12:55:55'
   deriver = udf.Deriver()
   deriver.set_parameters({udf.TIME_SEPARATOR: '@'})
   fmt = deriver.search(my_date)
   print(udf.as_parts(my_date, fmt))

gives the result

.. code-block:: text

   (2022, 2, 20)


YY Pivot
^^^^^^^^

The ``udf.YY_PIVOT`` property is used to determine how the century is applied to two-digit years.
By default, the ``undated`` pivot year is the current year minus 80.
The default Excel pivot year is set as 40.
The value should be a four-digit year. ``1940`` would mean any two-digit year 40 or over would be given the century ``19``.
Any two-digit year ``39`` and under will be given the century ``20``.

.. code-block:: python3

   import undated.fmts as udf
   my_date = '20-01-35'
   # Set the first deriver to 1940
   deriver1 = udf.Deriver()
   deriver1.set_parameters({udf.HINTS: [udf.Y2], udf.YY_PIVOT: 1940})
   fmt1 = deriver1.search(my_date)
   print(udf.as_parts(my_date, fmt1))
   # Now try again with the pivot at 1930
   deriver2 = udf.Deriver()
   deriver1.set_parameters({udf.HINTS: [udf.Y2], udf.YY_PIVOT: 1930})
   fmt2 = deriver1.search(my_date)
   print(udf.as_parts(my_date, fmt2))


gives the result

.. code-block:: text

   (2035, 1, 20)
   (1935, 1, 20)


String Formats
--------------

The ``UndatedFormat`` object is not designed to be created manually. So, if the date format is simple and known, string-based formats can be used.
These use the letters ``YyMmd`` along with the ``-`` character to indicate a separator.

- **Y**: 4-digit year
- **y**: 2-digit year
- **M**: the month as a string
- **m**: the month as an integer
- **d**: the day as an integer
- **-**: separator character, indicates a space, comma, dot, slash or dash

.. code-block:: python3

   import undated.fmts as udf
   print(udf.as_parts(2021_06_12, fmt='Ymd'))
   print(udf.as_parts('2021JUN12', fmt='YMd'))
   print(udf.as_parts('21JUN12', fmt='yMd'))
   print(udf.as_parts('12/JUN/2021', fmt='d-M-Y'))
   print(udf.as_parts('12-JUN-21', fmt='d-M-y'))
   print(udf.as_parts('12.06.21', fmt='d-m-y'))

gives the result (for each print)

.. code-block:: text

   (2021, 6, 12)

To go the next step and convert to ``ud.YMD`` or ``datetime``

.. code-block:: python3

   import datetime
   import undated as ud
   import undated.fmts as udf

   parts = udf.as_parts('12-JUN-21', fmt='d-M-y')
   print(ud.YMD(parts))
   print(datetime.datetime(*parts))

gives the result

.. code-block:: text

   20210612
   2021-06-12 00:00:00

