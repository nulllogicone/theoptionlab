using System;
using System.Collections.Generic;
using System.Text;

namespace OptionFunctions.Domain
{
    interface IStrategy
    {
        public void MakeCombo();
        public bool CheckExit();
    }
}
