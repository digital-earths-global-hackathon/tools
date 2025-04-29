# Downloading NERSC Hackathon (SCREAM) Data

Author: Andrew Gettelman, andrew.getelman@pnnl.gov

The SCREAM model and other NERSC served data (e.g. for feature tracking) is available from the NERSC node using [Globus](https://www.globus.org/)

Globus allows error checking and parallel transfers.


## Step 1: Set up a globus endpoint to receive data

- Many data centers already use Globus, if so, refer to the local documentation
- If not, there are instructions to use [globus connect](https://www.globus.org/globus-connect) on severs or a laptop
- You will also need to be a globus user (sign up to get a globus ID)

## Step 2: Navigate to the Globus Endpoint

- Log into your globus account on the web
- Navigate a browser to the [globus endpoint](https://app.globus.org/file-manager?origin_id=41bda5dc-c193-43e8-a922-0fe4f94490e7&origin_path=%2F&two_pane=true) for SCREAM (and other hackathon data)
- SCREAM data is in the `scream-cess-healpix` folder (you can get whole or just part of the data)
- Put in your endpoint on the right and start the transfer



