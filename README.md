# Background

This code was developed as part of the "WindWeasel: PLC Monitoring, Analysis, and Alerting System" project, funded by the U.S. Department of Energy (DOE) Wind Energy Technologies Office (WETO) to develop host-based intrusion detection systems for wind site and wind turbine controllers.

More information on the project is located in this [presentation](https://www.researchgate.net/publication/363693960_Hardening_Wind_Systems_from_Cyber_Threats_Wind_Cybersecurity_Workshop_WindWeasel_Wind_Controller_Monitoring_Analysis_and_Alerting_System). 

# Running the Code
The sofware was tested with Python 3.9.13, but should run with any Python 3.7+ version.

Install the dependent packages: 
```
pip install -r requirements.txt
```

Run the code: 
```
python Wind_HMI_IDS.py
```

The online version of the code interacts with the turbine HMI website, but the offline version of the code pulls static data from the local XML files. 

# Contributing
Developers are welcome. Please submit pull requests with detailed messages. 

# License
See [License.md](License.md). 

