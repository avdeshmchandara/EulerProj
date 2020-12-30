#
def print_logo():
    logo='''
    ************************************************
    *                    EULER                     *
    *    A SQLITE POWERED DATA SCIENCE TOOLKIT     *
    *          SINGH.AP79@GMAIL.NOSPAM.COM         * 
    ************************************************
    '''
    logo = '\n'.join([l.strip() for l in logo.split('\n')])
    print (logo)
print_logo()