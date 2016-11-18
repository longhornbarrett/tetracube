# tetracube
This is my implementation of the parser

Initially I got the Naive C Sharp code ported to java(very very easy almost a copy and paste).  This ran in approx 7 seconds
reliably on my laptop(to give me a benchmark for my machine).

Then my strategy was to implement a parser that read in raw bytes from the file.  Since the file was in ASCII
the data was very easy to handle in bytes.  It required no conversion(which is very expensive) into char's or Strings.
This allowed me to do artimetic using byte values( a little messy but quicker than converting into a long and back to bytes).
I also only went through the array once and held on to data positions for the processing.

The one built in class I used for computation was the Calendar object to get milliseconds since the epoch.  Given time I would
have probably implemented this myself as well to increase speed.

I tried to use as little helper code as posssible and implement most operations myself to keep them low level and fast.

Then I proceeded to change the code to multithread it.  I read in a buffer of bytes's then find the last newline byte
in the buffer and copy everything past that to a new buffer and pass that one off to a thread to start parsing while
the main thread goes back and continues reading from the input stream.
I kept the threads in a List so as they finished I grabbed their output and wrote it to the output stream in order so
the final file had the correct ordering.

I could not disable garbage collection in java(got removed around the 1.4 version) so I tried to keep references to everything
until the end so garbage collection would be mimimal.

The code consistently ran in approx 1.2 seconds on my laptop or 5 to 6 times faster than the Naive solution in java.

In c++ given time to refresh myself I could have gotten this to run even faster.