package com.urbanairship.hbackup;

import java.io.IOException;

public interface Transfer {
    abstract public void doTransfer() throws IOException;
}
