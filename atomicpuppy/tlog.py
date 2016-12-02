import sys

def extract(frame):
    got = []
    while frame is not None:
        got.append(frame)
        frame = frame.f_back
    got.reverse()
    return got


laststack = []


def tlog(stream, *msgs):
    """Outputs a log message with stack trace context."""
    global laststack
    try:
        raise Exception()
    except Exception:
        frame = sys.exc_info()[2].tb_frame.f_back
    stack = extract(frame)
    for i, fr in enumerate(stack):
        if not (i < len(laststack) and laststack[i] is fr):
            lineno = fr.f_lineno
            co = fr.f_code
            filename = co.co_filename
            name = co.co_name
            # print >> stream, " " * i + ("%i:%s:%s:%s" % (i, filename, lineno, name))
            stream.write(" " * i + ("%s:%s:%s:%i\n" % (filename, lineno, name, i)))
    stream.write(" " * len(stack) + " ".join(map(str, msgs)) + "\n")
    laststack = stack
